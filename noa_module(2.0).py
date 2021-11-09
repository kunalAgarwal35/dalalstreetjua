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
import function_store as fs
from discord_webhook import DiscordWebhook, DiscordEmbed
# import alice_orders as ao
from nsetools import Nse

# file_path = 'xlwings.xlsx'
# book = xw.Book(file_path)
# s = book.sheets['tick_backtest2']
# s.clear_contents()
market_open = datetime.time(9, 15, 00)
market_close = datetime.time(15, 30, 00)
# Last adjustment deadline on expiry day
trade_stop_time = datetime.time(14, 0, 0)
sq_off_time = datetime.time(15, 10, 0)

allins = pickle.load(open('june.instruments', 'rb'))
# allins.extend(pickle.load(open('july.instruments','rb')))
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
tdate = datetime.datetime(2021, 5, 1)
# When to adjust positions
percentile = 85
# Frequency of processing (seconds, upto 60)
freq = 15
# % of spot up and down to limit trading strikes
trade_range = 0.03
prob_range = 0.08
dayformat = "%y-%m-%d"
# index_name = 'NIFTY BANK'
index_name = 'NIFTY 50'
aux_name = 'pbelow2'
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


def make_curr_ins(timestamp):
    types = []
    segments = []
    exchanges = []
    ticksizes = []
    indices = []
    engts = {}
    ts = {}
    # expiry = find_expiry(timestamp)
    curr_ins = fs.get_curr_ins(timestamp)
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

    return curr_ins, vix_instrument, nifty_50_instrument, engts, ts


def find_trading_sessions(timestamp, expiry):
    t1 = time.time()
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = int(expopt * 75 - (
            timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300)
    return trading_sessions


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


vixnow = 0


def probability_below(timestamp, opt_ltps, opt_ois):
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
        exp_opt_date = datetime.datetime.strptime(item, '%Y-%m-%d')

        if exp_opt_date.date() >= timestamp.date():
            # print(time.time() - t1, 'find_expiry')
            return exp_opt_date.date()


def update_ltps(last_options_tick, opt_ltps, opt_ois):
    global spot, prob_range, nifty_50_instrument
    try:
        spot = last_options_tick[nifty_50_instrument['instrument_token']]['last_price']
    except:
        if spot:
            pass
        else:
            return opt_ltps, opt_ois
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
    return opt_ltps, opt_ois


start_date = datetime.datetime.strptime(os.listdir(option_tickdir)[0],dayformat)
curr_ins, vix_instrument, nifty_50_instrument, engts, ts = make_curr_ins(start_date)
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
print('Initiating')
pbelow = {}
dayticks = os.listdir(option_tickdir)
if 'lp' + aux_name in os.listdir(aux_files):
    last_processed = pickle.load(open(aux_files + 'lp' + aux_name, 'rb'))

for item in dayticks:
    print(item)
    try:
        if datetime.datetime.strptime(item, dayformat).date() < last_processed.date():
            continue
    except:
        pass
    temp_pbelow = {}
    tickdict = pickle.load(open(option_tickdir + item, 'rb'))
    curr_ins = {}
    for timestamp in tickdict.keys():
        last_options_tick = tickdict[timestamp]
        if not len(curr_ins):
            curr_ins, vix_instrument, nifty_50_instrument, engts, ts = make_curr_ins(timestamp)
        opt_ltps, opt_ois = update_ltps(last_options_tick, opt_ltps, opt_ois)
        if vix_instrument['instrument_token'] in last_options_tick.keys():
            vixnow = last_options_tick[vix_instrument['instrument_token']]['last_price']
        if not type(last_processed) == type('') and (
                timestamp <= last_processed or timestamp.second == last_processed.second):
            continue
        if timestamp.second % freq:
            continue
        print(timestamp)
        last_processed = timestamp
        if find_trading_sessions(timestamp, find_expiry(timestamp)):
            prob_below = probability_below(timestamp, opt_ltps, opt_ois)
            temp_pbelow[timestamp] = prob_below
            opt_ltps, opt_ois = {}, {}
    if 'pbelow2_' + insname + '_' in os.listdir(aux_files):
        pbelow = pickle.load(open(aux_files + 'pbelow2_' + insname + '_', 'rb'))
    for timestamp in temp_pbelow:
        pbelow[timestamp] = temp_pbelow[timestamp]
        last_processed = timestamp
    pickle.dump(last_processed, open(aux_files + 'lp' + aux_name, 'wb'))
    pickle.dump(pbelow, open(aux_files + 'pbelow2_' + insname + '_', 'wb'))
    pbelow = {}
    tickdict = {}
