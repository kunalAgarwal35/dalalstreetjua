import pickle
import datetime
import os
import threading
import time
from scipy.stats import norm
import math
import numpy as np
import pandas as pd
import statistics
import dateutil.parser
import concurrent.futures
import threading

vix = pickle.load(open('vix_historical.pkl','rb'))
nifty = pickle.load(open('nifty_historical.pkl','rb'))
banknifty = pickle.load(open('banknifty_historical.pkl','rb'))

def get_vix_close(timestamp):
    global vix
    if type(timestamp) == type(datetime.date(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year, timestamp.month, timestamp.day,15,25,00)
    # bn_token = 260105
    # ohlc = zd.get_historical(bn_token,datetime.datetime(2010,1,1),datetime.datetime.now(),"day",False)
    if timestamp not in vix['date'].to_list():
        timestamp = timestamp - datetime.timedelta(minutes=timestamp.minute % 5, seconds=timestamp.second)
        return float(vix['close'][vix['date'] == timestamp].to_list()[0])
    return float(vix['close'][vix['date']==timestamp].to_list()[0])

def get_nifty_close(timestamp):
    global nifty
    if type(timestamp) == type(datetime.date(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year, timestamp.month, timestamp.day,15,25,00)
    # bn_token = 260105
    # ohlc = zd.get_historical(bn_token,datetime.datetime(2010,1,1),datetime.datetime.now(),"day",False)
    if timestamp not in nifty['date'].to_list():
        timestamp = timestamp - datetime.timedelta(minutes = timestamp.minute%5, seconds = timestamp.second)
        return float(nifty['close'][nifty['date']==timestamp].to_list()[0])
    return float(nifty['close'][nifty['date']==timestamp].to_list()[0])

def find_trading_sessions(timestamp,expiry):
    if type(timestamp) == type(datetime.date(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year,timestamp.month,timestamp.day,15,30)
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = expopt * 75 - (timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300
    return int(trading_sessions)


def nifty_distribution_custom(vix_min, vix_max, trading_sessions,vix,nifty):
    vix = vix.loc[vix['date'].isin(nifty['date'])]
    nifty = nifty.loc[nifty['date'].isin(vix['date'])]
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


def get_stats_from_vix(timestamp,vix_range_percent,expiry):
    # expiry is date object repping next expiry
    global vix,nifty
    vixnow = get_vix_close(timestamp)
    vvix,nnifty = vix.copy(),nifty.copy()
    vvix = vvix[vvix['date']<timestamp]
    nnifty = nnifty[nnifty['date']<timestamp]
    mean,sd = 0,0
    trading_sessions = find_trading_sessions(timestamp,expiry)
    if not trading_sessions:
        return mean,sd
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    ndis = nifty_distribution_custom(vix_min, vix_max, trading_sessions,vvix,nnifty)
    # data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
    sd = statistics.pstdev(ndis)
    mean = ndis.mean()
    return mean,sd

def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


def sort_by_strike(calls):
    _ = {}
    __ = {}
    for contract in calls.keys():
        _[name_to_strike(contract)] = calls[contract]
        __[name_to_strike(contract)] = contract
    ret = {}
    _ = dict(sorted(_.items()))
    for key in _.keys():
        ret[__[key]] = _[key]
    return ret


def probability_model(opt_ltps, opt_ois,mean,sd):
    t1 = time.time()
    data = np.arange(round_down(-10), round_down(10), 0.01)
    cdfd = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        elif 'PE' in item:
            puts[item] = opt_ltps[item]

    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
    _ = list(calls.keys())
    ma = max((name_to_strike(_[0]),name_to_strike(_[len(_)-1])))
    mi = min((name_to_strike(_[0]),name_to_strike(_[len(_)-1])))
    pseudo_spot = (ma+mi)/2
    _ = list(puts.keys())
    ma = max(ma,max((name_to_strike(_[0]) , name_to_strike(_[len(_) - 1]))))
    mi = min(mi, min((name_to_strike(_[0]) , name_to_strike(_[len(_) - 1]))))

    xs = list(np.arange(mi,ma,10))
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
            ind = len(data) - (cdfd > pop).sum() - 1
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
            ind = len(data) - (cdfd > (1 - pop)).sum() - 1
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
    print(time.time() - t1, 'probability_below_new')
    return prob_below


def raw_cdf_model(spot,vix_min,vix_max,timestamp,expiry):
    global vix,nifty
    dist = sorted(nifty_distribution_custom(vix_min,vix_max,find_trading_sessions(timestamp,expiry),vix.copy()[vix['date']<timestamp],nifty.copy()[nifty['date']<timestamp]))
    df = pd.DataFrame()
    df['dist'] = dist
    df['spots'] = spot + df['dist']*spot/100
    start = int(min(df['spots']))
    start = start - start%10
    end = int(max(df['spots']))
    end = end + 10 - end%10
    data = np.arange(start,end,10)
    probs = [(df['spots']<i).sum()*100/len(df['spots']) for i in data]
    pbelow = {}
    pbelow.update(zip(data,probs))
    return pbelow



















