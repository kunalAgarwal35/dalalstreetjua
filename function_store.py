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
import pyximport
pyximport.install()
from cythonized_loop.pyx import new_probability_model
import concurrent.futures
import threading


def make_curr_ins_from_allins(allins):
    curr_ins = {}
    for ins in allins:
        curr_ins[ins['instrument_token']] = ins
    return curr_ins


def get_curr_ins(timestamp):
    fut_dir = 'C:/Kiteconnect/dayticks'
    opt_dir = 'C:/Kiteconnect/dayoptionticks'
    instrumentlist = os.listdir('C:/Kiteconnect')
    instrumentlist = [i for i in instrumentlist if '.instruments' in i]
    mtimes = {}
    insdict = {}
    for i in range(0, len(instrumentlist)):
        path = 'C:/Kiteconnect/' + instrumentlist[i]
        try:
            insdict[instrumentlist[i]] = pickle.load(open(path, 'rb'))
            mtimes[instrumentlist[i]] = (datetime.datetime.fromtimestamp(os.path.getmtime(path)))
        except:
            pass
    mtimes = dict(sorted(mtimes.items(), key=lambda x:x[1]))
    print(timestamp)
    mtimekeys = list(mtimes.keys())
    for i in range(0,len(mtimekeys)-1):
        if mtimes[mtimekeys[i]]<=timestamp<=mtimes[mtimekeys[i+1]]:
            allins = insdict[mtimekeys[i]]
            print(len(allins))
            cr = make_curr_ins_from_allins(allins)
            print('found')
            return cr
        if i == len(mtimekeys)-2:
            if timestamp > mtimes[mtimekeys[i+1]]:
                allins = insdict[mtimekeys[i+1]]
                print(len(allins))
                cr = make_curr_ins_from_allins(allins)
                print('found')
                return cr


def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


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


def probability_below(opt_ltps, opt_ois):
    sd = 1
    mean = 0
    data = np.arange(round_down(-30), round_down(30), 0.01)
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        elif 'PE' in item:
            puts[item] = opt_ltps[item]

    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
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


def probability_below_custom(opt_ltps, opt_ois,mean,sd):
    data = np.arange(round_down(-30), round_down(30), 0.01)
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        elif 'PE' in item:
            puts[item] = opt_ltps[item]

    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
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


def normal_curve_prob(pct_change):
    sd = 1
    mean = 0
    data = np.arange(round_down(-30), round_down(30), 0.01)
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    distnow = norm(loc=mean, scale=sd)
    probability = distnow.cdf(pct_change)

    return probability



def spread_evs_normal(opt_ltps, opt_ois,l,u):
    prob_ltps = {}
    prob_ois = {}
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    prob_below = probability_below(prob_ltps,prob_ois)
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


def spread_evs_custom(opt_ltps, opt_ois,l,u,mean,sd):
    prob_ltps = {}
    prob_ois = {}
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    prob_below = probability_below_custom(prob_ltps,prob_ois,mean,sd)
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


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


def filter_for_tradable_spreads(open_positions,put_spreads, call_spreads,l,u):
    open_call_spread, ctemp = [], []
    open_put_spread, ptemp = [], []
    if len(open_positions):
        for key in open_positions.keys():
            if 'CE' in key:
                ctemp.append(key)
            if 'PE' in key:
                ptemp.append(key)
        open_call_spread.append(ctemp[0] + '-' + ctemp[1])
        open_call_spread.append(ctemp[1] + '-' + ctemp[0])
        open_put_spread.append(ptemp[0] + '-' + ptemp[1])
        open_put_spread.append(ptemp[1] + '-' + ptemp[0])
    t1 = time.time()
    ret_put, ret_call = {}, {}
    for key in call_spreads.keys():
        strikes = key.split('-')
        strikes = [name_to_strike(i) for i in strikes]
        if l <= strikes[0] <= u and l <= strikes[1] <= u:
            ret_call[key] = call_spreads[key]
        if key in open_call_spread:
            ret_call[key] = call_spreads[key]
    for key in put_spreads.keys():
        strikes = key.split('-')
        strikes = [name_to_strike(i) for i in strikes]
        if l <= strikes[0] <= u and l <= strikes[1] <= u:
            ret_put[key] = put_spreads[key]
        if key in open_put_spread:
            ret_put[key] = put_spreads[key]
    # print(time.time() - t1, 'filter_for_tradable_spreads')
    ret_put = dict(sorted(ret_put.items(), key=lambda item: item[1]))
    ret_call = dict(sorted(ret_call.items(), key=lambda item: item[1]))
    for key in put_spreads.keys():
        keys = key.split('-')
        put_spreads[key] = round_down(put_spreads[key])
    for key in call_spreads.keys():
        keys = key.split('-')
        call_spreads[key] = round_down(call_spreads[key])
    return ret_put, ret_call


def update_ltp_oi_ohlc(timestamp,contracts):
    #contracts is a dictionary of ohlc data imported from csvs downloaded in nse historical format
    #timestamp is date for which ltps and ois are to be reported
    opt_ltps = {}
    opt_ois = {}
    try:
        for contract in contracts.keys():
            contracts[contract]['Date'] = pd.to_datetime(contracts[contract]['Date'],format = '%Y-%m-%d')
            contracts[contract]['Date'] = contracts[contract]['Date'].dt.date
            if timestamp in contracts[contract]['Date'].to_list():
                # opt_ltps[contract] = float(contracts[contract]['Settle Price'][contracts[contract]['Date'] == timestamp])
                opt_ltps[contract] = float(contracts[contract]['Close'][contracts[contract]['Date'] == timestamp])
                opt_ois[contract] = float(contracts[contract]['Open Interest'][contracts[contract]['Date'] == timestamp])
        return opt_ltps,opt_ois
    except:
        return opt_ltps,opt_ois

def update_ltp_oi_findata(timestamp,contracts,dateformat):
    opt_ltps = {}
    opt_ois = {}
    for contract in contracts.keys():
        try:
            contracts[contract]['timestamp'] = [datetime.datetime.strptime(i, dateformat) for i in
                                                contracts[contract]['timestamp']]
        except Exception as e:
            # print(e)
            pass
        if timestamp in contracts[contract]['timestamp'].to_list():
            if 'bid' not in contracts[contract].columns:
                opt_ltps[contract] = float(contracts[contract]['close'][contracts[contract]['timestamp'] == timestamp])
            else:
                opt_ltps[contract] = float(
                    contracts[contract]['ask'][contracts[contract]['timestamp'] == timestamp]) + float(
                    contracts[contract]['bid'][contracts[contract]['timestamp'] == timestamp])

            opt_ois[contract] = float(contracts[contract]['oi'][contracts[contract]['timestamp'] == timestamp])
    return opt_ltps, opt_ois

def update_ltp_oi_ohlc_settle(timestamp,contracts):
    #contracts is a dictionary of ohlc data imported from csvs downloaded in nse historical format
    #timestamp is date for which ltps and ois are to be reported
    opt_ltps = {}
    opt_ois = {}
    for contract in contracts.keys():

        contracts[contract]['Date'] = pd.to_datetime(contracts[contract]['Date'],format = '%Y-%m-%d')
        contracts[contract]['Date'] = contracts[contract]['Date'].dt.date
        if timestamp in contracts[contract]['Date'].to_list():
            opt_ltps[contract] = float(contracts[contract]['Settle Price'][contracts[contract]['Date'] == timestamp])
            # opt_ltps[contract] = float(contracts[contract]['Close'][contracts[contract]['Date'] == timestamp])
            opt_ois[contract] = float(contracts[contract]['Open Interest'][contracts[contract]['Date'] == timestamp])
    return opt_ltps,opt_ois


def find_expiry(timestamp,options_historical_dir):
    final_dayformat = '%y-%m-%d'
    for item in os.listdir(options_historical_dir):
        try:
            exp_opt_date = datetime.datetime.strptime(item,final_dayformat)
        except:
            exp_opt_date = dateutil.parser.parse(item)
        if type(timestamp) == type(datetime.date(2021,1,1)):
            pass
        elif type(timestamp) == type(datetime.datetime(2021,1,1)):
            timestamp = timestamp.date()
        if  exp_opt_date.date() >= timestamp:
            # print(time.time() - t1, 'find_expiry')
            return exp_opt_date.date()


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


def payoff_graph(open_positions, opt_ltps,s):
    for item in open_positions.keys():
        if item not in opt_ltps.keys():
            return
    if not len(open_positions):
        return
    calls = {}
    puts = {}
    for key in open_positions.keys():
        if 'CE' in key:
            calls[key] = open_positions[key]
        if 'PE' in key:
            puts[key] = open_positions[key]
    strikes = []
    for item in open_positions.keys():
        strikes.append(name_to_strike(item))

    diff = max(strikes) - min(strikes)
    strikes.append(min(strikes) - diff)
    strikes.append(max(strikes) + diff)
    strikes.sort()
    pnl = {}
    for expiry in strikes:
        pnl[expiry] = 0
        for key in calls.keys():
            pnl[expiry] += find_existing_positions_result({key:calls[key]},opt_ltps[key],expiry)
        for key in puts.keys():
            pnl[expiry] += find_existing_positions_result({key:puts[key]},opt_ltps[key],expiry)

    resultdf = pd.DataFrame(columns = ['spot','positions'])
    for sp in pnl.keys():
        resultdf = resultdf.append({'spot':sp,'positions':pnl[sp]},ignore_index=True)
    s.range('I17:K26').clear()
    s.range('I17').value = resultdf
    # return resultdf
    # figure = resultdf.plot(x='spot', y='positions', grid=1, label='PnL',).get_figure()
    # s.pictures.add(figure, name='Payoff Graph', update=True,left=s.range('B5').left, top=s.range('B5').top)
    # opt_ltps = {}
    # for key in open_positions.keys():
    #     opt_ltps[key] = (name_to_strike(key)/100)*(name_to_strike(key)/100)


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


def find_trades(open_positions, put_spreads, call_spreads,percentile):
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
                        break
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
                        break
        except KeyError:
            pass
    keys = list(tradelist.keys())
    for key in keys:
        if tradelist[key] == 0:
            tradelist.pop(key)
    # print(time.time() - t1, 'find_trades')
    return tradelist


def initiate_variables(new,aux_name,aux_files):
    if 'ledger_'+aux_name in os.listdir(aux_files) and new:
        ledger = pickle.load(open(aux_files + 'ledger_'+aux_name, 'rb'))
    else:
        ledger = pd.DataFrame(columns = ['expiry','instrument','oid','price','qty','value'])

    if 'open_positions_'+aux_name in os.listdir(aux_files) and new:
        open_positions = pickle.load(open(aux_files + 'open_positions_'+aux_name, 'rb'))
    else:
        open_positions = {}
    if 'pnldf_'+aux_name in os.listdir(aux_files) and new:
        pnldf = pickle.load(open(aux_files + 'pnldf_'+aux_name, 'rb'))
    else:
        pnldf = pd.DataFrame()
    if 'lp' + aux_name in os.listdir(aux_files) and new:
        last_processed = pickle.load(open(aux_files + 'lp' + aux_name, 'rb'))
    else:
        last_processed = ''
    return ledger, open_positions,pnldf,last_processed


def square_off_all(open_positions,lot_size):
    tradelist = {}
    for key in open_positions.keys():
        tradelist[key] = -open_positions[key]/lot_size
    return tradelist


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

def stock_distribution(trading_sessions,df):
    nnifty = df.copy()
    nnifty['ret'] = nnifty['close'].shift(int(-trading_sessions))
    nnifty = nnifty[nnifty['ret'].notna()]
    nnifty.reset_index(drop=True, inplace=True)
    ret_distribution = (100 * np.log(nnifty['ret'] / nnifty['close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    # print(time.time() - t1, 'nifty_distribution')
    return ret_distribution

def find_trading_sessions(timestamp,expiry):
    if type(timestamp) == type(datetime.date(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year,timestamp.month,timestamp.day,15,30)
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = expopt * 75 - (
                timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300
    return int(trading_sessions)
def get_stats_from_vix(timestamp,vix_range_percent,vixnow,expiry):
    # expiry is date object repping next expiry
    global vix,nifty
    if type(timestamp) == type(datetime.datetime(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year,timestamp.month,timestamp.day)
        vix = vix[vix['date']<timestamp]
        nifty = nifty[nifty['date']<timestamp]
    mean,sd = 0,0
    trading_sessions = find_trading_sessions(timestamp,expiry)
    if not trading_sessions:
        return mean,sd
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    ndis = nifty_distribution_custom(vix_min, vix_max, trading_sessions,vix,nifty)
    # data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
    sd = statistics.pstdev(ndis)
    mean = ndis.mean()
    return mean,sd


def get_stats_from_vix_bn(timestamp,vix_range_percent,vixnow,expiry):
    # expiry is date object repping next expiry
    global vix, banknifty
    if type(timestamp) == type(datetime.datetime(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year,timestamp.month,timestamp.day)
        vix = vix[vix['date']<timestamp]
        nifty = banknifty[banknifty['date']<timestamp]
    mean,sd = 0,0
    trading_sessions = find_trading_sessions(timestamp,expiry)
    if not trading_sessions:
        return mean,sd
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    ndis = nifty_distribution_custom(vix_min, vix_max, trading_sessions,vix,nifty)
    data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
    sd = statistics.pstdev(ndis)
    mean = ndis.mean()
    return mean,sd



def get_banknifty_close(timestamp):
    fname = 'get_historical/2010-01-012021-08-01(260105)day.csv'
    df = pd.read_csv(fname)
    df['date'] = df[['date']].apply(pd.to_datetime)
    # ohlc = df
    if type(timestamp) == type(datetime.date(2020,1,1)):
        timestamp = datetime.datetime(timestamp.year, timestamp.month, timestamp.day)
    # bn_token = 260105
    # ohlc = zd.get_historical(bn_token,datetime.datetime(2010,1,1),datetime.datetime.now(),"day",False)
    return float(df['close'][df['date']==timestamp].to_list()[0])

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


def realized_vs_projected(spreads,expiry_spot,opt_ltps):
    m_realized = 0
    m_projected = 0
    for spread in spreads.keys():
        m_realized += find_spread_result(spread,expiry_spot,opt_ltps)
        m_projected += spreads[spread]
    return m_realized,m_projected


def find_spread_result(spread,expiry_spot,opt_ltps):
    spread = spread.split('-')
    s0, s1 = spread[0],spread[1]
    res = find_existing_positions_result({s0:1},opt_ltps[s0],expiry_spot)
    res += find_existing_positions_result({s1:-1},opt_ltps[s1],expiry_spot)
    return res

def find_spread_results(put_spreads,call_spreads,expiry_spot,opt_ltps):
    put_spreads_result, call_spreads_result = {}, {}
    for spread in put_spreads.keys():
        put_spreads_result[spread] = find_spread_result(spread,expiry_spot,opt_ltps)
    for spread in call_spreads.keys():
        call_spreads_result[spread] = find_spread_result(spread,expiry_spot,opt_ltps)
    return put_spreads_result,call_spreads_result


def get_contracts(instrument,timestamp):
    dayformat = '%y-%m-%d'
    options_historical = 'nse_options_historical/'
    instrument = instrument + '/'
    expiry = find_expiry(timestamp, options_historical + instrument)
    contracts = {}
    contracts_list = os.listdir(options_historical + instrument + expiry.strftime(dayformat))
    for contract in contracts_list:
        contracts[contract.replace('.csv','')] = pd.read_csv(options_historical+instrument+expiry.strftime(dayformat)+'/'+contract)
    return contracts


# def new_probability_model(opt_ltps, opt_ois,mean,sd):
#     cdef float t1 = time.time()
#     cdef float[:] data = np.arange(round_down(-10), round_down(10), 0.01)
#     cdef float[:] cdfd = norm.cdf(data, loc=mean, scale=sd)
#     cdef dict puts = {}
#     cdef dict calls = {}
#     for item in opt_ltps.keys():
#         if 'CE' in item:
#             calls[item] = opt_ltps[item]
#         elif 'PE' in item:
#             puts[item] = opt_ltps[item]
#
#     calls = sort_by_strike(calls)
#     puts = sort_by_strike(puts)
#     _ = list(calls.keys())
#     ma = max((name_to_strike(_[0]),name_to_strike(_[len(_)-1])))
#     mi = min((name_to_strike(_[0]),name_to_strike(_[len(_)-1])))
#     pseudo_spot = (ma+mi)/2
#     _ = list(puts.keys())
#     ma = max(ma,max((name_to_strike(_[0]) , name_to_strike(_[len(_) - 1]))))
#     mi = min(mi, min((name_to_strike(_[0]) , name_to_strike(_[len(_) - 1]))))
#
#     xs = list(np.arange(mi,ma,10))
#     prob_below = {}
#     oik = {}
#     oiv = {}
#     distnow = norm(loc=mean, scale=sd)
#     for i in range(0, len(calls.keys()) - 2):
#         for j in range(i + 1, len(calls.keys()) - 1):
#             keyi = list(calls.keys())[i]
#             keyj = list(calls.keys())[j]
#             credit_collected = calls[keyi] - calls[keyj]
#             width = int(keyj.replace('CE', '')) - int(keyi.replace('CE', ''))
#             pop = 1 - credit_collected / width
#             be = int(keyi.replace('CE', '')) + credit_collected
#             ind = len(data) - (cdfd > pop).sum() - 1
#             pct = data[ind]
#             center = be - be * (pct / 100)
#             for x in xs:
#                 if x not in oik.keys():
#                     oik[x] = []
#                     oiv[x] = []
#                 x_pct = 100 * np.log(x / center)
#                 probability = distnow.cdf(x_pct)
#                 oik[x].append(min(opt_ois[keyi], opt_ois[keyj]))
#                 oiv[x].append(probability)
#     for i in range(0, len(puts.keys()) - 2):
#         for j in range(i + 1, len(puts.keys()) - 1):
#             keyi = list(puts.keys())[i]
#             keyj = list(puts.keys())[j]
#             credit_collected = puts[keyj] - puts[keyi]
#             width = int(keyj.replace('PE', '')) - int(keyi.replace('PE', ''))
#             pop = 1 - credit_collected / width
#             be = int(keyj.replace('PE', '')) - credit_collected
#             ind = len(data) - (cdfd > (1 - pop)).sum() - 1
#             pct = data[ind]
#             center = be - be * (pct / 100)
#             for x in xs:
#                 if x not in oik.keys():
#                     oik[x] = []
#                     oiv[x] = []
#                 x_pct = 100 * np.log(x / center)
#                 probability = distnow.cdf(x_pct)
#                 oik[x].append(min(opt_ois[keyi], opt_ois[keyj]))
#                 oiv[x].append(probability)
#     for x in xs:
#         cum_probability = (pd.Series(oik[x]) * pd.Series(oiv[x])).sum()
#         cum_probability = 100 * cum_probability / (pd.Series(oik[x]).sum())
#         prob_below[x] = cum_probability
#     prob_below = dict(sorted(prob_below.items()))
#     print(time.time() - t1, 'probability_below_new')
#     return prob_below


def spread_evs_normal_new_model(opt_ltps, opt_ois,l,u):
    prob_ltps = {}
    prob_ois = {}
    mean,sd = 0,1
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    prob_below = new_probability_model(prob_ltps,prob_ois,mean,sd)
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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


def spread_evs_custom_new_model(opt_ltps, opt_ois,l,u,mean,sd):
    prob_ltps = {}
    prob_ois = {}
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    prob_below = new_probability_model(prob_ltps,prob_ois,mean,sd)
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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


def spread_evs_custom_new_model_with_pbelow(opt_ltps, opt_ois,l,u,mean,sd):
    prob_ltps = {}
    prob_ois = {}
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps,opt_ois = prob_ltps,prob_ois
    prob_below = new_probability_model(prob_ltps,prob_ois,mean,sd)
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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
    return put_spreads, call_spreads, prob_below


def simple_prob_model(mean,sd,spot):
    puts = {}
    calls = {}
    mi = 0.85 * spot - 0.85 * spot % 100
    ma = 1.15 * spot - 1.15 * spot % 100
    xs = list(np.arange(mi,ma,10))
    prob_below = {}
    distnow = norm(loc=mean, scale=sd)
    for x in xs:
        prob_below[x] = 100*distnow.cdf(100*np.log(x/spot))
    prob_below = dict(sorted(prob_below.items()))
    return prob_below


def spread_evs_simple(opt_ltps,opt_ois,l,u,mean,sd,spot):
    prob_ltps = {}
    prob_ois = {}
    for key in opt_ltps.keys():
        if l < name_to_strike(key) < u:
            prob_ltps[key] = opt_ltps[key]
            prob_ois[key] = opt_ois[key]
    opt_ltps, opt_ois = prob_ltps, prob_ois
    prob_below = simple_prob_model(mean, sd, spot)
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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
            area_between = 0
            prob_prices = list(prob_below.keys())
            for k in range(0, len(prob_below) - 1):
                price = prob_prices[k]
                gain = find_spread_result(buykey + '-' + sellkey, price, opt_ltps)
                if min(buy_strike, sell_strike) < price < max(buy_strike, sell_strike):
                    prob = prob_below[prob_prices[k - 1]] - prob_below[prob_prices[k + 1]]
                    area_between += prob * gain / 100
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
    return put_spreads, call_spreads, prob_below


vix = pickle.load(open('vix_historical.pkl','rb'))
nifty = pickle.load(open('nifty_historical.pkl','rb'))
banknifty = pickle.load(open('banknifty_ronit','rb'))

