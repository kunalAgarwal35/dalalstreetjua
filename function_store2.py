import pickle
import datetime
import os
import threading
import time
from scipy.stats import norm, gaussian_kde
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

vix = vix.loc[vix['date'].isin(nifty['date'])]
nifty = nifty.loc[nifty['date'].isin(vix['date'])]
vix.reset_index(inplace=True, drop=True)
nifty.reset_index(inplace=True, drop=True)


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

def merge_dfs(dfs):
    return pd.concat(dfs, axis=1)

def add_prefix_to_columns(df,prefix):
    return df.rename(columns={col: prefix + col for col in df.columns})

def nifty_distribution_custom(vix_min, vix_max, trading_sessions,merged_df):
    merged_df['nifty_ret'] = merged_df['nifty_close'].shift(int(-trading_sessions))
    merged_df = merged_df[merged_df['nifty_ret'].notna()]
    nv = merged_df.loc[(merged_df['vix_open'] < vix_max) & (merged_df['vix_open'] > vix_min)]
    nv.reset_index(inplace=True,drop=True)
    ret_distribution = (100 * np.log(nv['nifty_ret'] / nv['nifty_close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    # print(time.time() - t1, 'nifty_distribution')
    return ret_distribution

def nifty_distribution_custom_old(vix_min, vix_max, trading_sessions,vix,nifty):
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


def spread_evs_from_pbelow(opt_ltps, opt_ois, prob_below,trade_range):
    prob_ltps = {}
    prob_ois = {}
    Keymax = max(zip(opt_ltps.values(), opt_ltps.keys()))[1]
    valmax = max(zip(opt_ltps.values(), opt_ltps.keys()))[0]
    opt_type = 'CE' if 'CE' in Keymax else 'PE'
    if opt_type == 'CE':
        syn_spot = name_to_strike(Keymax) + valmax
    else:
        syn_spot = name_to_strike(Keymax) - valmax
    l,u = syn_spot*(1-trade_range),syn_spot*(1+trade_range)
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        else:
            puts[item] = opt_ltps[item]
    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
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
    for key in put_spreads.keys():
        put_spreads[key] = round_down(put_spreads[key])
    for key in call_spreads.keys():
        call_spreads[key] = round_down(call_spreads[key])
    # print(time.time() - t1, 'spread_evs')
    return put_spreads, call_spreads

def add_debit_spreads(put_spreads, call_spreads):
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
        put_spreads[key] = round_down(put_spreads[key])
    for key in call_spreads.keys():
        keys = key.split('-')
        call_spreads[key] = round_down(call_spreads[key])
    return put_spreads, call_spreads

def realized_vs_projected(spreads,expiry_spot,opt_ltps):
    m_realized = 0
    m_projected = 0
    for spread in spreads.keys():
        m_realized += find_spread_result(spread,expiry_spot,opt_ltps)
        m_projected += spreads[spread]
    return m_realized,m_projected

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

def find_spread_result(spread,expiry_spot,opt_ltps):
    spread = spread.split('-')
    s0, s1 = spread[0],spread[1]
    res = find_existing_positions_result({s0:1},opt_ltps[s0],expiry_spot)
    res += find_existing_positions_result({s1:-1},opt_ltps[s1],expiry_spot)
    return res

def pbelow_to_distribution(pbelowdict):
    dist = pd.DataFrame()
    dist['spot'] = list(pbelowdict.keys())
    dist['pbelow'] = list(pbelowdict.values())
    dist['pbelow_shift'] = dist['pbelow'].shift(1)
    dist['pbelow_shift2'] = dist['pbelow_shift'].fillna(0)
    dist['dist'] = dist['pbelow'] - dist['pbelow_shift']
    sumdist = (dist['pbelow'] - dist['pbelow_shift2']).sum()
    dist = dist[['spot', 'dist']]
    return dist, sumdist

def spread_evs_from_dist(opt_ltps, opt_ois, dist,trade_range):
    prob_ltps = {}
    prob_ois = {}
    Keymax = max(zip(opt_ltps.values(), opt_ltps.keys()))[1]
    valmax = max(zip(opt_ltps.values(), opt_ltps.keys()))[0]
    opt_type = 'CE' if 'CE' in Keymax else 'PE'
    if opt_type == 'CE':
        syn_spot = name_to_strike(Keymax) + valmax
    else:
        syn_spot = name_to_strike(Keymax) - valmax
    l,u = syn_spot*(1-trade_range),syn_spot*(1+trade_range)
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        else:
            puts[item] = opt_ltps[item]
    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
    call_spreads = {}
    put_spreads = {}
    scipy_kde = gaussian_kde(dist)
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
            area_between = 0.5 * (scipy_kde.integrate_box_1d(np.log(sell_strike/syn_spot), np.log(buy_strike/syn_spot))) * (
                    profit_below_sell + profit_above_buy)
            area_below_sell = profit_below_sell * (scipy_kde.integrate_box_1d(-100,np.log(sell_strike/syn_spot)))
            area_above_buy = profit_above_buy * (scipy_kde.integrate_box_1d(np.log(buy_strike/syn_spot),100))
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
            area_between = 0.5 * (scipy_kde.integrate_box_1d(np.log(buy_strike/syn_spot), np.log(sell_strike/syn_spot))) * (
                    profit_above_sell + profit_below_buy)
            area_above_sell = profit_above_sell * scipy_kde.integrate_box_1d(np.log(sell_strike/syn_spot),100)
            area_below_buy = profit_below_buy * scipy_kde.integrate_box_1d(-100,np.log(buy_strike/syn_spot))
            spread = buykey + '-' + sellkey
            put_spreads[spread] = area_above_sell + area_between + area_below_buy
    put_spreads = dict(sorted(put_spreads.items(), key=lambda item: item[1]))
    call_spreads = dict(sorted(call_spreads.items(), key=lambda item: item[1]))
    for key in put_spreads.keys():
        put_spreads[key] = round_down(put_spreads[key])
    for key in call_spreads.keys():
        call_spreads[key] = round_down(call_spreads[key])
    # print(time.time() - t1, 'spread_evs')
    return put_spreads, call_spreads


merged_df = merge_dfs([add_prefix_to_columns(vix, 'vix_'), add_prefix_to_columns(nifty, 'nifty_')])


















