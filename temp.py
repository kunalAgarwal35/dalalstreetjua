from scipy.stats import norm
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb
import zd
import datetime
import pandas as pd
import time
import statistics
import math
import pickle
import os
import xlwings as xw
# import subprocess
#
# subprocess.Popen(['python',"ticksim.py"], close_fds=True)
# time.sleep(5)

file_path='xlwings.xlsx'
s=xw.Book(file_path).sheets['Sheet3']
# allins = zd.kite.instruments()
allins = pickle.load(open('june.instruments','rb'))
market_open = datetime.time(9,15,00)
market_close = datetime.time(15,30,00)

iupac = "%m%d%Y-%H%M%S-%f"
dateformat = '%Y-%m-%d'


last_tick={}
ltp_by_symbol={}
def nifty_distribution(vix_min, vix_max, trading_sessions):
    t1=time.time()
    nv=vix.loc[(vix['open']<vix_max) & (vix['open']>vix_min)]
    nnifty = nifty.copy()
    nnifty['ret']=nnifty['close'].shift(int(-trading_sessions))
    nnifty = nnifty[nnifty['ret'].notna()]
    nnifty=nnifty[nnifty['date'].isin(nv['date'].to_list())]
    nnifty.reset_index(drop=True,inplace=True)
    ret_distribution=(100*np.log(nnifty['ret']/nnifty['close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    return ret_distribution

def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


def make_curr_ins(allins):
    curr_ins = {}
    types = []
    segments = []
    exchanges = []
    ticksizes = []
    indices = []
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
    return curr_ins, types,segments,exchanges,ticksizes,indices
def find_trading_sessions(timestamp):
    expopt = np.busday_count(timestamp.date(), find_expiry(timestamp).date()) + 1
    trading_sessions = expopt * 75 - (
                timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300
    return trading_sessions
curr_ins, types,segments,exchanges,ticksizes,indices = make_curr_ins(allins)
nifty50list = pd.read_csv('ind_nifty50list.csv')['Symbol'].to_list()

for index in indices:
    if index['name']=='INDIA VIX':
        # print(index)
        vix_instrument=index
        break
for index in indices:
    if index['name']=='NIFTY 50':
        # print(index)
        nifty_50_instrument=index
        break

fdate=datetime.datetime(2011,1,1)
tdate=datetime.datetime(2021,6,27)

vix=zd.get_historical(vix_instrument['instrument_token'],fdate,tdate, "5minute",0)
nifty=zd.get_historical(nifty_50_instrument['instrument_token'],fdate,tdate, "5minute",0)
vix=vix.loc[vix['date'].isin(nifty['date'])]
nifty=nifty.loc[nifty['date'].isin(vix['date'])]
#
# vix_min = 15
# vix_max = 16
# trading_sessions = expopt*75 - (timestamp - datetime.datetime.combine(timestamp.date(),datetime.time(9,15,00))).seconds/300
# ndis = nifty_distribution(vix_min, vix_max, trading_sessions)
# sd = statistics.pstdev(ndis)
# mean = ndis.mean()

# data = np.arange(-5,5,0.01)
# pdf = norm.pdf(data,loc = mean , scale = sd)
# cdf = norm.cdf(data,loc = mean , scale = sd)
# pd.Series(data).plot.hist(bins = 500)
# pd.Series(ndis).plot.hist(bins = 50)
# pd.Series(pdf).plot.hist(bins = 50)
# sb.set_style('whitegrid')
# sb.lineplot(data, pdf , color = 'black')
# plt.xlabel('Heights')
# plt.ylabel('Probability Density')
# data = np.arange(1,10,0.01)
# pdf = norm.pdf(data , loc = 5.3 , scale = 1 )
# norm(loc = 0.8 , scale = 1.1).cdf(0.7)
option_tickdir = 'juneoptionticks/'


def probability_below(fname,opt_ltps):
    last_options_tick = pickle.load(open(option_tickdir + fname, 'rb'))
    timestamp = datetime.datetime.strptime(fname,iupac)
    vixtoken = vix_instrument['instrument_token']
    vixnow = last_options_tick[vixtoken]['last_price']
    vix_min = 0.85*vixnow
    vix_max = 1.15*vixnow
    trading_sessions = find_trading_sessions(timestamp)
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
        if curr_ins[item]['instrument_type'] in ['CE', 'PE']:
            opt_ltps[str(int(curr_ins[item]['strike']))+curr_ins[item]['instrument_type']] = last_options_tick[item]['last_price']
            oi[str(int(curr_ins[item]['strike']))+curr_ins[item]['instrument_type']] = last_options_tick[item]['oi']
            if curr_ins[item]['instrument_type'] == 'CE':
                calls[str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']] = last_options_tick[item]['last_price']
            else:
                puts[str(int(curr_ins[item]['strike'])) + curr_ins[item]['instrument_type']] = last_options_tick[item]['last_price']
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
    for x in xs:
        oiclaims_keys = []
        oiclaims_values = []
        for i in range(0,len(calls.keys())-2):
            for j in range(i+1,len(calls.keys())-1):
                keyi = list(calls.keys())[i]
                keyj = list(calls.keys())[j]
                # print(keyi,keyj)
                credit_collected = calls[keyi] - calls[keyj]
                width = int(keyj.replace('CE','')) - int(keyi.replace('CE',''))
                pop = 1 - credit_collected/width
                be = int(keyi.replace('CE','')) + credit_collected
                ind = len(data) - (cdf > pop).sum() -1
                pct = data[ind]
                center = be - be*(pct/100)
                # print(center)
                # print('Credit Collected:',credit_collected,' Breakeven: ',be,' POP: ',pop)
                x_pct = 100*np.log(x/center)
                # print(x_pct)
                probability = norm(loc = mean , scale = sd).cdf(x_pct)
                oiclaims_keys.append(min(oi[keyi],oi[keyj]))
                oiclaims_values.append(probability)
        for i in range(0,len(puts.keys())-2):
            for j in range(i+1,len(puts.keys())-1):
                keyi = list(puts.keys())[i]
                keyj = list(puts.keys())[j]
                # print(keyi,keyj)
                credit_collected = puts[keyj] - puts[keyi]
                width = int(keyj.replace('PE','')) - int(keyi.replace('PE',''))
                pop = 1 - credit_collected/width
                be = int(keyj.replace('PE','')) - credit_collected
                ind = len(data) - (cdf > (1-pop)).sum() -1
                pct = data[ind]
                center = be - be*(pct/100)
                # print('Credit Collected:',credit_collected,' Breakeven: ',be,' POP: ',pop)
                x_pct = 100*np.log(x/center)
                probability = norm(loc = mean , scale = sd).cdf(x_pct)
                oiclaims_keys.append(min(oi[keyi],oi[keyj]))
                oiclaims_values.append(probability)

        cum_probability = (pd.Series(oiclaims_keys)*pd.Series(oiclaims_values)).sum()
        cum_probability = 100*cum_probability/(pd.Series(oiclaims_keys).sum())
        prob_below[x] = cum_probability
    prob_below = dict(sorted(prob_below.items()))
    return prob_below,opt_ltps

def spread_evs(prob_below,opt_ltps):
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
    for i in range(0,len(calls)-2):
        for j in range(i+1,len(calls)-1):
            buykey = list(calls.keys())[j]
            sellkey = list(calls.keys())[i]
            credit_collected = calls[sellkey] - calls[buykey]
            sell_strike = int(sellkey.replace('CE', ''))
            buy_strike = int(buykey.replace('CE', ''))
            width = buy_strike - sell_strike
            be = buy_strike + credit_collected
            profit_below_sell = credit_collected
            profit_above_buy = credit_collected - width
            area_between = 0.5*(prob_below[buy_strike] - prob_below[sell_strike])*(profit_below_sell+profit_above_buy)/10000
            area_below_sell = profit_below_sell*(prob_below[sell_strike])/100
            area_above_buy = profit_above_buy*(100-prob_below[buy_strike])/100
            spread = buykey + '-' + sellkey
            call_spreads[spread] = area_below_sell+area_between+area_above_buy
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
    return put_spreads,call_spreads



def find_expiry(timestamp):
    options_historical_folder = '2021options'
    expiries = [datetime.datetime.strptime(item,dateformat) for item in os.listdir(options_historical_folder)]
    expiries.sort()
    for expiry in expiries:
        if timestamp<=expiry:
            return expiry

ledger = pd.DataFrame()
def show_tradelist(tradelist,timestamp,option_ltps):
    global ledger, open_positions
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = 1.01 * option_ltps[key]
        else:
            price = 0.99 * option_ltps[key]
        nr = {'timestamp': timestamp, 'expiry': find_expiry(timestamp).date(), 'instrument': key,
              'qty': tradelist[key] * 75, 'price': price}
        ledger = ledger.append(nr, ignore_index=True)
        if key in open_positions.keys():
            open_positions[key] += tradelist[key]*75
        else:
            open_positions[key] = tradelist[key]*75
    poplist=[]
    for key in open_positions.keys():
        if open_positions[key] == 0:
            poplist.append(key)
    for key in poplist:
        open_positions.pop(key)
    # print(tradelist)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    sqpnl = 0
    for key in open_positions.keys():
        sqpnl += option_ltps[key]*open_positions[key]

    booked_pnl = {}
    poplist = []
    for contract in print_ledger['instrument']:
        if contract not in booked_pnl.keys():
            if print_ledger[print_ledger['instrument']==contract]['qty'].sum() == 0:
                booked_pnl[contract] = print_ledger[print_ledger['instrument']==contract]['value'].sum()
        else:
            if print_ledger[print_ledger['instrument'] == contract]['qty'].sum() != 0:
               poplist.append(contract)
    for key in poplist:
        booked_pnl.pop(key)
    openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
    s.range('F10').value = booked_pnl
    s.range('H9').value = 'Net Booked P/L'
    s.range('H10').value = sum(booked_pnl.values())
    s.range('H4').value = 'Open P/L'
    s.range('H5').value = openpnl
    s.range('Z1').value = print_ledger['value'].sum() + sqpnl

open_positions = {}
percentile = 60
ltps = {}
for last_file in os.listdir(option_tickdir):
    timewithoutdate= datetime.datetime.strptime(last_file,iupac).time()
    timestamp = datetime.datetime.strptime(last_file,iupac)
    if timewithoutdate.second%60 != 0:
        continue
    if market_open < timewithoutdate < market_close and datetime.datetime.strptime(last_file,iupac).date()>datetime.date(2021,6,17):
        try:
            s.range('K1').value = timestamp
            m0 = time.time()
            prb, ltps = probability_below(last_file,ltps)
            put_spreads, call_spreads = spread_evs(prb, ltps)
            s.range('A1').value = dict(sorted(put_spreads.items()))
            s.range('C1').value = dict(sorted(call_spreads.items()))
            m1 = time.time()
            # print('Spread EVS:',m1-m0)
            if len(put_spreads)*len(call_spreads) == 0:
                continue
        except KeyError:
            continue
        print(datetime.datetime.strptime(last_file,iupac))
        _ = list(call_spreads.keys())
        for key in _:
            s1 = key[:key.index('-')]
            s2 = key[key.index('-')+1:]
            spread2 = s2+'-'+s1
            call_spreads[spread2] = - call_spreads[key]
        _ = list(put_spreads.keys())
        for key in _:
            s1 = key[:key.index('-')]
            s2 = key[key.index('-')+1:]
            spread2 = s2+'-'+s1
            put_spreads[spread2] = - put_spreads[key]
        put_spreads = dict(sorted(put_spreads.items(), key=lambda item: item[1]))
        call_spreads = dict(sorted(call_spreads.items(), key=lambda item: item[1]))
        tradelist = {}
        if len(open_positions) == 0:
            for key in call_spreads.keys():
                if call_spreads[key] == max(call_spreads.values()):
                    s1 = key[:key.index('-')]
                    s2 = key[key.index('-')+1:]
                    tradelist[s1] = 1
                    tradelist[s2] = -1

            for key in put_spreads.keys():
                if put_spreads[key] == max(put_spreads.values()):
                    s1 = key[:key.index('-')]
                    s2 = key[key.index('-')+1:]
                    tradelist[s1] = 1
                    tradelist[s2] = -1
        else:
            call_spread_ev_thresh = np.percentile(pd.Series(call_spreads.values()), 100 - percentile)
            put_spread_ev_thresh = np.percentile(pd.Series(put_spreads.values()), 100 - percentile)
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
        m2 = time.time()
        # print('Tradelist Created: ',m2-m1)
        if len(tradelist) > 0:
            print(tradelist)
        show_tradelist(tradelist,datetime.datetime.strptime(last_file,iupac),ltps)
        last_ts = timestamp
        # print('Loop Complete:', time.time()-m2)



