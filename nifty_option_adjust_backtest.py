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
from selenium import webdriver
import zd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from discord_webhook import DiscordWebhook, DiscordEmbed

file_path = 'xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['tick_backtest']
s.clear_contents()
market_open = datetime.time(9, 15, 00)
market_close = datetime.time(15, 30, 00)
allins = pickle.load(open('july.instruments','rb'))
allins2 = pickle.load(open('june.instruments','rb'))
iupac = "%m%d%Y-%H%M%S-%f"
option_tickdir = 'optionticks/'
aux_files = 'optionadjustbacktest/'
wbhk = 'https://discordapp.com/api/webhooks/833393245870489631/sMqbM-uxq5ojp8tK8Wt_oHN4b0GNe6Rggk_PgS1UVbb1TtvQOY_mkLfOFCozvEjdydnW'
# Parameters:
# VIX range % (up and down)
vix_range_percent = .15
# VIX and NIFTY historical sample range
fdate = datetime.datetime(2011, 1, 1)
tdate = datetime.datetime(2021,1,1)
# When to adjust positions
percentile = 75
# Frequency of processing (seconds, upto 60)
freq = 30
manual_range = [15000, 16200]
dayformat="%y-%m-%d"

def pingdiscord(txt):
    return


def tradable_strikes():
    return manual_range


def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


def nifty_distribution(vix_min, vix_max, trading_sessions):
    t1 = time.time()
    t1 = time.time()
    nv = vix.loc[(vix['open'] < vix_max) & (vix['open'] > vix_min)]
    nnifty = nifty.copy()
    nnifty['ret'] = nnifty['close'].shift(int(-trading_sessions))
    nnifty = nnifty[nnifty['ret'].notna()]
    nnifty = nnifty[nnifty['date'].isin(nv['date'].to_list())]
    nnifty.reset_index(drop=True, inplace=True)
    ret_distribution = (100 * np.log(nnifty['ret'] / nnifty['close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    print(time.time() - t1, 'nifty_distribution')
    return ret_distribution


def make_curr_ins(allins):
    t1 = time.time()
    curr_ins = {}
    types = []
    segments = []
    exchanges = []
    ticksizes = []
    indices = []
    engts = {}
    ts = {}

    for ins in allins:
        curr_ins[ins['instrument_token']] = ins
    for ins in curr_ins.keys():
        if curr_ins[ins]['segment'] == 'INDICES':
            indices.append(curr_ins[ins])
    for ins in allins2:
        curr_ins[ins['instrument_token']] = ins
    for ins in curr_ins.keys():
        if curr_ins[ins]['segment'] == 'INDICES':
            indices.append(curr_ins[ins])
    for index in indices:
        if index['name'] == 'INDIA VIX':
            # print(index)
            vix_instrument = index
            break
    for index in indices:
        if index['name'] == 'NIFTY 50':
            # print(index)
            nifty_50_instrument = index
            break
    print(time.time() - t1, 'make_curr_ins')
    return curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, nifty_50_instrument, engts, ts


def find_trading_sessions(timestamp, expiry):
    t1 = time.time()
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = expopt * 75 - (
            timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300
    return trading_sessions
    print(time.time() - t1, 'find_trading_sessions')


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


vixnow = 0



def probability_below(timestamp, last_options_tick, opt_ltps):
    t1 = time.time()
    global vixnow
    if not vixnow:
        return
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    expiry = find_expiry()
    trading_sessions = find_trading_sessions(timestamp, expiry)
    ndis = nifty_distribution(vix_min, vix_max, trading_sessions)
    data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
    sd = statistics.pstdev(ndis)
    mean = ndis.mean()
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    oi = {}
    for item in last_options_tick.keys():
        if curr_ins[item]['instrument_type'] in ['CE', 'PE'] and curr_ins[item]['name'] == 'NIFTY':
            oi[str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']] = last_options_tick[item]['oi']
            if curr_ins[item]['instrument_type'] == 'CE':
                calls[str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']] = last_options_tick[item][
                    'last_price']
            else:
                puts[str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']] = last_options_tick[item][
                    'last_price']
        else:
            if curr_ins[item]['name'] == 'NIFTY 50':
                spot = last_options_tick[item]['last_price']

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
                oik[x].append(min(oi[keyi], oi[keyj]))
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
                oik[x].append(min(oi[keyi], oi[keyj]))
                oiv[x].append(probability)
    for x in xs:
        cum_probability = (pd.Series(oik[x]) * pd.Series(oiv[x])).sum()
        cum_probability = 100 * cum_probability / (pd.Series(oik[x]).sum())
        prob_below[x] = cum_probability
    prob_below = dict(sorted(prob_below.items()))
    print(time.time() - t1, 'probability_below_new')
    return prob_below, opt_ltps

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

def filter_for_tradable_spreads(put_spreads, call_spreads):
    t1 = time.time()
    [l, u] = tradable_strikes()
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



def copy_and_sort_spreads(put_spreads, call_spreads, bidaskspread):
    t1 = time.time()
    _ = list(call_spreads.keys())
    for key in _:
        s1 = key[:key.index('-')]
        s2 = key[key.index('-') + 1:]
        spread2 = s2 + '-' + s1
        call_spreads[spread2] = - call_spreads[key]
    _ = list(put_spreads.keys())
    for key in _:
        s1 = key[:key.index('-')]
        s2 = key[key.index('-') + 1:]
        spread2 = s2 + '-' + s1
        put_spreads[spread2] = - put_spreads[key]
    put_spreads = dict(sorted(put_spreads.items(), key=lambda item: item[1]))
    call_spreads = dict(sorted(call_spreads.items(), key=lambda item: item[1]))
    for key in put_spreads.keys():
        keys = key.split('-')
        put_spreads[key] -= (bidaskspread[keys[0]]+bidaskspread[keys[1]])
        put_spreads[key] = round_down(put_spreads[key])
    for key in call_spreads.keys():
        keys = key.split('-')
        call_spreads[key] -= (bidaskspread[keys[0]]+bidaskspread[keys[1]])
        call_spreads[key] = round_down(call_spreads[key])
    print(time.time() - t1, 'copy_and_sort_spreads')
    return put_spreads, call_spreads


def initiate_variables():
    if 'ledger' in os.listdir(aux_files):
        ledger = pickle.load(open(aux_files + 'ledger', 'rb'))
    else:
        ledger = pd.DataFrame()

    if 'open_positions' in os.listdir(aux_files):
        open_positions = pickle.load(open(aux_files + 'open_positions', 'rb'))
    else:
        open_positions = {}
    return ledger, open_positions


def find_trades(open_positions, put_spreads, call_spreads):
    t1 = time.time()
    tradelist = {}
    if len(open_positions) == 0:
        for key in call_spreads.keys():
            if call_spreads[key] == max(call_spreads.values()):
                s1 = key[:key.index('-')]
                s2 = key[key.index('-') + 1:]
                tradelist[s1] = 1
                tradelist[s2] = -1
        for key in put_spreads.keys():
            if put_spreads[key] == max(put_spreads.values()):
                s1 = key[:key.index('-')]
                s2 = key[key.index('-') + 1:]
                tradelist[s1] = 1
                tradelist[s2] = -1
    else:
        call_spread_ev_thresh = np.percentile(pd.Series(call_spreads.values()), percentile)
        put_spread_ev_thresh = np.percentile(pd.Series(put_spreads.values()), percentile)
        cspr = {}
        pspr = {}
        for key in open_positions.keys():
            if 'CE' in key:
                if open_positions[key] > 0:
                    cspr[0] = key
                else:
                    cspr[1] = key
            else:
                if open_positions[key] > 0:
                    pspr[0] = key
                else:
                    pspr[1] = key
        try:
            curcallspr = cspr[0] + '-' + cspr[1]
            if call_spreads[curcallspr] < call_spread_ev_thresh and max(call_spreads.values()) > 20:
                tradelist[cspr[0]] = -1
                tradelist[cspr[1]] = 1
                for key in call_spreads.keys():
                    if call_spreads[key] == max(call_spreads.values()):
                        s1 = key[:key.index('-')]
                        s2 = key[key.index('-') + 1:]
                        if s1 not in list(tradelist.keys()):
                            tradelist[s1] = 1
                        else:
                            tradelist[s1] += 1
                        if s2 not in list(tradelist.keys()):
                            tradelist[s2] = -1
                        else:
                            tradelist[s2] += -1
        except KeyError:
            pass
        try:
            curputspr = pspr[0] + '-' + pspr[1]
            if put_spreads[curputspr] < put_spread_ev_thresh and max(put_spreads.values()) > 20:
                tradelist[pspr[0]] = -1
                tradelist[pspr[1]] = 1
                for key in put_spreads.keys():
                    if put_spreads[key] == max(put_spreads.values()):
                        s1 = key[:key.index('-')]
                        s2 = key[key.index('-') + 1:]
                        if s1 not in list(tradelist.keys()):
                            tradelist[s1] = 1
                        else:
                            tradelist[s1] += 1
                        if s2 not in list(tradelist.keys()):
                            tradelist[s2] = -1
                        else:
                            tradelist[s2] += -1
        except KeyError:
            pass
    keys = list(tradelist.keys())
    for key in keys:
        if tradelist[key] == 0:
            tradelist.pop(key)
    print(time.time() - t1, 'find_trades')
    return tradelist


def find_expiry(timestamp):
    t1 = time.time()
    for item in os.listdir('2021options'):
        exp_opt_date = datetime.datetime.strptime(item,'%Y-%m-%d')
        if  exp_opt_date >= timestamp:
            print(time.time() - t1, 'find_expiry')
            return exp_opt_date.date()



def show_tradelist(ledger, open_positions, tradelist, timestamp, option_ltps, xl_put_spreads, xl_call_spreads, ts):
    t1 = time.time()
    expiry = find_expiry(timestamp)
    nrlist = []
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = option_ltps[key] + bidaskspread[key]
            typ = 'buy'
        else:
            price = option_ltps[key] - bidaskspread[key]
            typ = 'sell'
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * 75, 'price': price,
              'oid': time.time()}
        nrlist.append(nr)
    time.sleep(1)
    for nr in nrlist:
        ledger = ledger.append(nr, ignore_index=True)
        key = nr['instrument']
        if key in open_positions.keys():
            open_positions[key] += tradelist[key] * 75
        else:
            open_positions[key] = tradelist[key] * 75
    poplist = []
    for key in open_positions.keys():
        if open_positions[key] == 0:
            poplist.append(key)
    for key in poplist:
        open_positions.pop(key)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    sqpnl = 0
    for key in open_positions.keys():
        sqpnl += option_ltps[key] * open_positions[key]

    booked_pnl = {}
    poplist = []
    for contract in print_ledger['instrument']:
        if contract not in booked_pnl.keys():
            if print_ledger[print_ledger['instrument'] == contract]['qty'].sum() == 0:
                booked_pnl[contract] = print_ledger[print_ledger['instrument'] == contract]['value'].sum()
        else:
            if print_ledger[print_ledger['instrument'] == contract]['qty'].sum() != 0:
                poplist.append(contract)
    for key in poplist:
        booked_pnl.pop(key)

    openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
    pnldict = {'Open P/L': openpnl, 'Booked P/L': sum(booked_pnl.values()),
               'Net P/L': print_ledger['value'].sum() + sqpnl}

    # book.app.screen_updating = False
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    s.range('F6').value = pnldict
    s.range('F16').value = booked_pnl
    s.range('A1').value = xl_put_spreads
    s.range('C1').value = xl_call_spreads
    # book.app.screen_updating = True
    print(time.time() - t1, 'show_tradelist')
    return ledger, open_positions


def update_ltps(last_options_tick, opt_ltps):
    t1 = time.time()
    for item in last_options_tick.keys():
        if curr_ins[item]['instrument_type'] in ['CE', 'PE']:
            opt_ltps[str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']] = last_options_tick[item][
                'last_price']
    print(time.time() - t1, 'update_ltps')
    return opt_ltps


def bid_ask_spread(last_options_tick, bidaskspread):
    t1 = time.time()
    for token in last_options_tick.keys():
        ins = curr_ins[token]
        if ins['instrument_type'] in ['CE','PE']:
            bid = last_options_tick[token]['depth']['buy'][0]['price']
            ask = last_options_tick[token]['depth']['sell'][0]['price']
            bidaskspread[str(int(ins['strike']))+ins['instrument_type']] = round_down(abs(ask - bid))
    print(time.time() - t1, 'bid_ask_spread')
    return bidaskspread

def square_off_all():
    global open_positions
    tradelist = {}
    for key in open_positions.keys():
        tradelist[key] = -open_positions[key]
    return tradelist

curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, nifty_50_instrument, engts, ts = make_curr_ins(
    allins)
nifty50list = pd.read_csv('ind_nifty50list.csv')['Symbol'].to_list()
print('Getting Historical Data')
vix = zd.get_historical(vix_instrument['instrument_token'], fdate.date(), tdate.date(), "5minute", 0)
nifty = zd.get_historical(nifty_50_instrument['instrument_token'], fdate.date(), tdate.date(), "5minute", 0)
vix = vix.loc[vix['date'].isin(nifty['date'])]
nifty = nifty.loc[nifty['date'].isin(vix['date'])]

opt_ltps = {}
bidaskspread = {}
ledger, open_positions = initiate_variables()

s.range('J1').value = 1
print('Initiating')
trading_days = os.listdir('dayoptionticks')

for day in trading_days:
    print(day)
    day_ticks = pickle.load(open('dayoptionticks/'+day,'rb'))
    for timestamp in day_ticks.keys():
        print(timestamp)
        try:
            if timestamp.date() == find_expiry(timestamp) and timestamp.time() > datetime.datetime.time(13,0,0):
                if len(open_positions):
                    tradelist = square_off_all()
                    opt_ltps = update_ltps(day_ticks[timestamp],opt_ltps)
                    ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp, opt_ltps,
                                                            xl_put_spreads, xl_call_spreads, ts)
                else:
                    continue

            if not vixnow:
                last_options_tick = day_ticks[timestamp]
                bidaskspread = bid_ask_spread(last_options_tick,bidaskspread)
                try:
                    time.sleep(0.1)
                    vixnow = last_options_tick[vix_instrument['instrument_token']]['last_price']
                except:
                    print('Error 2')
                    continue
            if not timestamp.second % freq and timestamp.time() > market_open:
                try:
                    time.sleep(0.1)
                    last_options_tick = day_ticks[timestamp]
                    bidaskspread = bid_ask_spread(last_options_tick, bidaskspread)
                    try:
                        vixnow = last_options_tick[vix_instrument['instrument_token']]['last_price']
                    except:
                        print('Error 3')
                        pass
                    prob_below, opt_ltps = probability_below(timestamp, last_options_tick, opt_ltps)
                except TypeError:
                    print('Error 4')
                    continue
                if len(prob_below):
                    try:
                        put_spreads, call_spreads = spread_evs(prob_below, opt_ltps)
                    except:
                        print('Error 7')
                        continue
                else:
                    continue
                try:
                    put_spreads, call_spreads = filter_for_tradable_spreads(put_spreads, call_spreads)
                except:
                    print('Error in filter_for_tradable_spreads')
                    continue

                xl_put_spreads = dict(sorted(put_spreads.items()))
                xl_call_spreads = dict(sorted(call_spreads.items()))
                for key in xl_put_spreads.keys():
                    try:
                        xl_put_spreads[key] = round_down(xl_put_spreads[key])
                    except:
                        pass
                for key in xl_call_spreads.keys():
                    try:
                        xl_call_spreads[key] = round_down(xl_call_spreads[key])
                    except:
                        pass
                try:
                    put_spreads, call_spreads = copy_and_sort_spreads(put_spreads, call_spreads, bidaskspread)
                    tradelist = find_trades(open_positions, put_spreads, call_spreads)
                    ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp, opt_ltps,
                                                            xl_put_spreads, xl_call_spreads, ts)
                except KeyError:
                    print('Error 5')
                    continue


                if len(tradelist) > 0:
                    pingdiscord(str(tradelist))
                    pickle.dump(open_positions, open(aux_files + 'open_positions', 'wb'))
                    pickle.dump(ledger, open(aux_files + 'ledger', 'wb'))
        except Exception as e:
            print(e)
            pass


