import pickle
import os
import time
import datetime
import pandas as pd
import numpy as np
from scipy.stats import norm
import xlwings as xw
import statistics
import math
import zd
from discord_webhook import DiscordWebhook, DiscordEmbed
# import alice_orders as ao
from nsetools import Nse

file_path = 'xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['noha']
s.clear_contents()
market_open = datetime.time(9, 15, 00)
market_close = datetime.time(15, 30, 00)
#Last adjustment deadline on expiry day
trade_stop_time = datetime.time(14,0,0)
sq_off_time = datetime.time(15,10,0)

allins = pickle.load(open('june.instruments','rb'))
allins.extend(pickle.load(open('july.instruments','rb')))
iupac = "%m%d%Y-%H%M%S-%f"
option_tickdir = 'dayoptionticks/'
options_historical_dir = '2021options/'
aux_files = 'optionadjustbacktest/'
# wbhk = 'https://discordapp.com/api/webhooks/833393245870489631/sMqbM-uxq5ojp8tK8Wt_oHN4b0GNe6Rggk_PgS1UVbb1TtvQOY_mkLfOFCozvEjdydnW'
# Parameters:
# VIX range % (up and down)
vix_range_percent = .2
# VIX and NIFTY historical sample range
fdate = datetime.datetime(2011, 1, 1)
tdate = datetime.datetime(2021,5,1)
# When to adjust positions
percentile = 85
# Frequency of processing (seconds, upto 60)
freq = 15
# % of spot up and down to limit trading strikes
trade_range = 0.03
prob_range = 0.08
dayformat="%y-%m-%d"
# index_name = 'NIFTY BANK'
index_name = 'NIFTY 50'
aux_name = 'noha'
# insname = 'BANKNIFTY'
insname = 'NIFTY'
lot_size = 75

def round_down(x):
    a = 0.01
    return math.floor(x / a) * a

def nifty_distribution(vix_min, vix_max, trading_sessions):
    t1 = time.time()
    nv = vix.loc[(vix['open'] < vix_max) & (vix['open'] > vix_min)]
    nnifty = nifty.copy()
    nnifty['ret'] = nnifty['close'].shift(int(-trading_sessions))
    nnifty = nnifty[nnifty['ret'].notna()]
    nnifty = nnifty[nnifty['date'].isin(nv['date'].to_list())]
    nnifty.reset_index(drop=True, inplace=True)
    ret_distribution = (100 * np.log(nnifty['ret'] / nnifty['close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    # print(time.time() - t1, 'nifty_distribution')
    return ret_distribution

def make_curr_ins(allins):
    curr_ins = {}
    types = []
    segments = []
    exchanges = []
    ticksizes = []
    indices = []
    engts = {}
    ts = {}
    # expiry = find_expiry(timestamp)

    for ins in allins:
        curr_ins[ins['instrument_token']] = ins
    for ins in curr_ins.keys():
        if curr_ins[ins]['instrument_type'] not in types:
            types.append(curr_ins[ins]['instrument_type'])
        if curr_ins[ins]['segment'] not in segments:
            segments.append(curr_ins[ins]['segment'])
        if curr_ins[ins]['exchange'] not in exchanges:
            exchanges.append(curr_ins[ins]['exchange'])
        if curr_ins[ins]['tick_size'] not in ticksizes:
            ticksizes.append(curr_ins[ins]['tick_size'])
        if curr_ins[ins]['segment'] == 'INDICES':
            indices.append(curr_ins[ins])
    for index in indices:
        if index['name'] == 'INDIA VIX':
            # print(index)
            vix_instrument = index
            break
    for index in indices:
        if index['name'] == index_name:
            # print(index)
            nifty_50_instrument = index
            break
    # for key in curr_ins.keys():
    #     ts[curr_ins[key]['tradingsymbol']] = curr_ins[key]['instrument_token']
    #     if curr_ins[key]['instrument_type'] in ['CE', 'PE'] and curr_ins[key]['name'] == 'BANKNIFTY' and curr_ins[key]['expiry'] == expiry:
    #         engts[str(int(curr_ins[key]['strike'])) + curr_ins[key]['instrument_type']] = curr_ins[key]['tradingsymbol']

    return curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, nifty_50_instrument, engts, ts

def find_trading_sessions(timestamp, expiry):
    t1 = time.time()
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = int(expopt * 75 - (
            timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300)
    return trading_sessions


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))

vixnow = 0

def probability_below(timestamp, opt_ltps,opt_ois):
    t1 = time.time()
    global vixnow
    if not vixnow:
        return
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    expiry = find_expiry(timestamp)
    trading_sessions = find_trading_sessions(timestamp, expiry)
    ndis = nifty_distribution(vix_min, vix_max, trading_sessions)
    data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
    sd = statistics.pstdev(ndis)
    mean = ndis.mean()
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        elif 'PE' in item:
            puts[item] = opt_ltps[item]

    calls = dict(sorted(calls.items()))
    puts = dict(sorted(puts.items()))
    xs = []
    for key in opt_ltps.keys():
        if int(key.replace('CE', '').replace('PE', '')) not in xs:
            xs.append(int(key.replace('CE', '').replace('PE', '')))
    prob_below = {}
    oik = {}
    oiv = {}
    distnow = norm(loc=mean, scale=sd)
    for i in range(0, len(calls.keys()) - 2):
        for j in range(i + 1, len(calls.keys()) - 1):
            keyi = list(calls.keys())[i]
            keyj = list(calls.keys())[j]
            credit_collected = calls[keyi] - calls[keyj]
            width = int(keyj.replace('CE', '')) - int(keyi.replace('CE', ''))
            pop = 1 - credit_collected / width
            be = int(keyi.replace('CE', '')) + credit_collected
            ind = len(data) - (cdf > pop).sum() - 1
            pct = data[ind]
            center = be - be * (pct / 100)
            for x in xs:
                if x not in oik.keys():
                    oik[x] = []
                    oiv[x] = []
                x_pct = 100 * np.log(x / center)
                probability = distnow.cdf(x_pct)
                oik[x].append(min(opt_ois[keyi], opt_ois[keyj]))
                oiv[x].append(probability)
    for i in range(0, len(puts.keys()) - 2):
        for j in range(i + 1, len(puts.keys()) - 1):
            keyi = list(puts.keys())[i]
            keyj = list(puts.keys())[j]
            credit_collected = puts[keyj] - puts[keyi]
            width = int(keyj.replace('PE', '')) - int(keyi.replace('PE', ''))
            pop = 1 - credit_collected / width
            be = int(keyj.replace('PE', '')) - credit_collected
            ind = len(data) - (cdf > (1 - pop)).sum() - 1
            pct = data[ind]
            center = be - be * (pct / 100)
            for x in xs:
                if x not in oik.keys():
                    oik[x] = []
                    oiv[x] = []
                x_pct = 100 * np.log(x / center)
                probability = distnow.cdf(x_pct)
                oik[x].append(min(opt_ois[keyi], opt_ois[keyj]))
                oiv[x].append(probability)
    for x in xs:
        cum_probability = (pd.Series(oik[x]) * pd.Series(oiv[x])).sum()
        cum_probability = 100 * cum_probability / (pd.Series(oik[x]).sum())
        prob_below[x] = cum_probability
    prob_below = dict(sorted(prob_below.items()))
    # print(time.time() - t1, 'probability_below_new')
    return prob_below

def find_expiry(timestamp):
    t1 = time.time()
    for item in os.listdir(options_historical_dir):
        exp_opt_date = datetime.datetime.strptime(item,'%Y-%m-%d')

        if  exp_opt_date.date() >= timestamp.date():
            # print(time.time() - t1, 'find_expiry')
            return exp_opt_date.date()

def update_ltps(last_options_tick, opt_ltps,opt_ois):
    global spot,prob_range,nifty_50_instrument
    try:
        spot = last_options_tick[nifty_50_instrument['instrument_token']]['last_price']
    except:
        if spot:
            pass
        else:
            return opt_ltps,opt_ois
    ll = spot * (1 - prob_range)
    ul = spot * (1 + prob_range)
    for item in last_options_tick.keys():
        try:
            if curr_ins[item]['instrument_type'] in ['CE', 'PE'] and curr_ins[item]['name'] == insname:
                if ll < curr_ins[item]['strike'] < ul:
                    contractname = str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']
                    opt_ltps[contractname] = (
                                                                                                                   last_options_tick[
                                                                                                                       item][
                                                                                                                       'depth'][
                                                                                                                       'buy'][
                                                                                                                       0][
                                                                                                                       'price'] +
                                                                                                                   last_options_tick[
                                                                                                                       item][
                                                                                                                       'depth'][
                                                                                                                       'sell'][
                                                                                                                       0][
                                                                                                                       'price']) / 2
                    opt_ois[contractname] = last_options_tick[item]['oi']
        except Exception as e:
            print(e)
            pass
    return opt_ltps,opt_ois

def find_existing_positions_result(pos,price, expiry):
    name = list(pos.keys())[0]
    pnl = 0
    strike = name_to_strike(name)
    diff = abs(expiry - strike)
    if 'CE' in name:
        if expiry > strike:
            return (diff - price)*pos[name]
        else:
            return -price*pos[name]
    elif 'PE' in name:
        if expiry > strike:
            return -price * pos[name]
        else:
            return (diff - price) * pos[name]
    return pnl

def instrument_real_expiries(instoken,expiry_date):
    ohlc = zd.get_historical(instoken, expiry_date, expiry_date, "5minute", 0)
    return float(ohlc['close'].to_list()[-1])

def realized_vs_projected(spreads,expiry_spot,opt_ltps):
    m_realized = 0
    m_projected = 0
    for spread in spreads.keys():
        m_realized += find_spread_result(spread,expiry_spot,opt_ltps)
        m_projected += spreads[spread]
    return m_realized,m_projected

def spread_evs(prob_below, opt_ltps):
    t1 = time.time()
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        else:
            puts[item] = opt_ltps[item]
    calls = dict(sorted(calls.items()))
    puts = dict(sorted(puts.items()))
    call_spreads = {}
    put_spreads = {}
    for i in range(0, len(calls) - 2):
        for j in range(i + 1, len(calls) - 1):
            buykey = list(calls.keys())[j]
            sellkey = list(calls.keys())[i]
            credit_collected = calls[sellkey] - calls[buykey]
            sell_strike = int(sellkey.replace('CE', ''))
            buy_strike = int(buykey.replace('CE', ''))
            width = buy_strike - sell_strike
            be = buy_strike + credit_collected
            profit_below_sell = credit_collected
            profit_above_buy = credit_collected - width
            area_between = 0.5 * (prob_below[buy_strike] - prob_below[sell_strike]) * (
                    profit_below_sell + profit_above_buy) / 10000
            area_below_sell = profit_below_sell * (prob_below[sell_strike]) / 100
            area_above_buy = profit_above_buy * (100 - prob_below[buy_strike]) / 100
            spread = buykey + '-' + sellkey
            call_spreads[spread] = area_below_sell + area_between + area_above_buy
    for i in range(0, len(puts) - 2):
        for j in range(i + 1, len(puts) - 1):
            buykey = list(puts.keys())[i]
            sellkey = list(puts.keys())[j]
            credit_collected = puts[sellkey] - puts[buykey]
            sell_strike = int(sellkey.replace('PE', ''))
            buy_strike = int(buykey.replace('PE', ''))
            width = sell_strike - buy_strike
            be = sell_strike - credit_collected
            profit_above_sell = credit_collected
            profit_below_buy = credit_collected - width
            area_between = 0.5 * (prob_below[sell_strike] - prob_below[buy_strike]) * (
                    profit_above_sell + profit_below_buy) / 10000
            area_above_sell = profit_above_sell * (100 - prob_below[sell_strike]) / 100
            area_below_buy = profit_below_buy * (prob_below[buy_strike]) / 100
            spread = buykey + '-' + sellkey
            put_spreads[spread] = area_above_sell + area_between + area_below_buy
    put_spreads = dict(sorted(put_spreads.items(), key=lambda item: item[1]))
    call_spreads = dict(sorted(call_spreads.items(), key=lambda item: item[1]))
    # print(time.time() - t1, 'spread_evs')
    return put_spreads, call_spreads

def find_spread_result(spread,expiry_spot,opt_ltps):
    spread = spread.split('-')
    s0, s1 = spread[0],spread[1]
    res = find_existing_positions_result({s0:1},opt_ltps[s0],expiry_spot)
    res += find_existing_positions_result({s1:-1},opt_ltps[s1],expiry_spot)
    return res


def tradable_strikes(spot):
    global trade_range
    return [round_down(spot*(1-trade_range)),round_down(spot*(1+trade_range))]


def filter_for_tradable_spreads(put_spreads, call_spreads):
    global spot
    t1 = time.time()
    [l, u] = tradable_strikes(spot)
    ret_put, ret_call = {}, {}
    for key in call_spreads.keys():
        strikes = key.split('-')
        strikes = [name_to_strike(i) for i in strikes]
        if l <= strikes[0] <= u and l <= strikes[1] <= u:
            ret_call[key] = call_spreads[key]
    for key in put_spreads.keys():
        strikes = key.split('-')
        strikes = [name_to_strike(i) for i in strikes]
        if l <= strikes[0] <= u and l <= strikes[1] <= u:
            ret_put[key] = put_spreads[key]
    # print(time.time() - t1, 'filter_for_tradable_spreads')
    return ret_put, ret_call







curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, nifty_50_instrument, engts, ts = make_curr_ins(allins)
print('Getting Historical Data')
vix = zd.get_historical(vix_instrument['instrument_token'], fdate.date(), tdate.date(), "5minute", 0)
nifty = zd.get_historical(nifty_50_instrument['instrument_token'], fdate.date(), tdate.date(), "5minute", 0)
vix = vix.loc[vix['date'].isin(nifty['date'])]
nifty = nifty.loc[nifty['date'].isin(vix['date'])]

opt_ltps = {}
opt_ois = {}
ol_tradelist = {}
spot = 0
last_processed = ''
# s.range('J1').value = 1
print('Initiating')
pbelow = {}
noa_result = pd.DataFrame()
nifty_expiries = {}
dayticks = os.listdir(option_tickdir)
adjustedhours = []
pbelow = pickle.load(open(aux_files+'pbelow_'+insname+'_','rb'))
if 'lp_ev_test' + aux_name in os.listdir(aux_files):
    last_processed = pickle.load(open(aux_files + 'lp_ev_test' + aux_name, 'rb'))
if 'noa_result' + aux_name in os.listdir(aux_files):
    noa_result = pickle.load(open(aux_files + 'noa_result' + aux_name, 'rb'))
for item in dayticks:
    print(item)
    try:
        if datetime.datetime.strptime(item,dayformat).date() < last_processed.date():
            continue
    except:
        pass

    # if not s.range('J1').value:
    #     break
    tickdict = pickle.load(open(option_tickdir+item,'rb'))
    for timestamp in tickdict.keys():

        # if not s.range('J1').value:
        #     break
        adj = timestamp.replace(minute = 0,second = 0, microsecond = 0)
        if adj in adjustedhours:
            continue
        last_options_tick = tickdict[timestamp]
        opt_ltps,opt_ois = update_ltps(last_options_tick, opt_ltps,opt_ois)
        if (not type(last_processed) == type('')) and (timestamp <= last_processed or timestamp.second == last_processed.second):
            continue
        if timestamp in pbelow.keys():
            print(timestamp)
            prob_below = pbelow[timestamp]
            put_spreads, call_spreads = spread_evs(prob_below, opt_ltps)
            put_spreads, call_spreads = filter_for_tradable_spreads(put_spreads, call_spreads)
            call_spreads = dict(sorted(call_spreads.items(), key=lambda item: item[1]))
            put_spreads = dict(sorted(put_spreads.items(), key=lambda item: item[1]))
            if not (len(call_spreads)*len(put_spreads)):
                continue
            key = list(call_spreads.keys())[-1]
            call_spreads = {key:call_spreads[key]}
            key = list(put_spreads.keys())[-1]
            put_spreads = {key: put_spreads[key]}
            if timestamp.date() not in nifty_expiries.keys():
                expiry_spot = instrument_real_expiries(nifty_50_instrument['instrument_token'],find_expiry(timestamp))
            else:
                expiry_spot = nifty_expiries[timestamp.date()]
            m_realized_calls, m_projected_calls = realized_vs_projected(call_spreads,expiry_spot,opt_ltps)
            m_realized_puts, m_projected_puts = realized_vs_projected(put_spreads,expiry_spot,opt_ltps)

            nr = {'timestamp':timestamp,'call_ev': m_projected_calls,'call_result':m_realized_calls,
                  'put_ev': m_projected_puts,'put_result':m_realized_puts,
                  'net_ev': m_projected_calls+m_projected_puts,
                  'net_result':m_realized_calls+m_realized_puts}

            noa_result = noa_result.append(nr,ignore_index=True)
            noa_result['cum_net_ev'] = noa_result['net_ev'].cumsum()
            noa_result['cum_net_realized'] = noa_result['net_result'].cumsum()

            if (not timestamp.minute % 15) and (not timestamp.second % 15):
                pickle.dump(last_processed, open(aux_files + 'lp_ev_test' + aux_name, 'wb'))
                pickle.dump(noa_result, open(aux_files + 'noa_result' + aux_name, 'wb'))
                s.range('A1').value = noa_result
            opt_ltps, opt_ois = {},{}
            adjustedhours.append(adj)
        elif timestamp > list(pbelow.keys())[-1]:
            print('Waiting for pbelow to catch up')
            time.sleep(100)
            pbelow = pickle.load(open(aux_files+'pbelow_'+insname+'_','rb'))



