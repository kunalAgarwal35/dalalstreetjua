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
s = book.sheets['banknifty_backtest']
s.clear_contents()
market_open = datetime.time(9, 15, 00)
market_close = datetime.time(15, 30, 00)
iupac = "%m%d%Y-%H%M%S-%f"
aux_files = 'optionadjustbacktest/'
# Parameters:
# VIX range % (up and down)
vix_range_percent = .15
# VIX and NIFTY historical sample range
fdate = datetime.datetime(2011, 1, 1)
tdate = datetime.datetime(2020,1,1)
# When to adjust positions
percentile = 25
dayformat="%y-%m-%d"
options_historical_dir = 'findata-master/BANKNIFTY/options/'
#Percentage away from spot strikes to consider
strike_range = 0.5
trade_range = 0.03
prob_range = 0.1
csv_database = {}

def pingdiscord(txt):
    wbhook = DiscordWebhook(url=wbhk, content=txt)
    response = wbhook.execute()


def tradable_strikes():
    return manual_range


def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


def nifty_distribution(vix_min, vix_max, trading_sessions):
    nv = vix.loc[(vix['open'] < vix_max) & (vix['open'] > vix_min)]
    nnifty = banknifty.copy()
    nnifty['ret'] = nnifty['close'].shift(int(-trading_sessions))
    nnifty = nnifty[nnifty['ret'].notna()]
    nnifty = nnifty[nnifty['date'].isin(nv['date'].to_list())]
    nnifty.reset_index(drop=True, inplace=True)
    ret_distribution = (100 * np.log(nnifty['ret'] / nnifty['close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    return ret_distribution


def make_curr_ins():
    allins = zd.kite.instruments()
    curr_ins = {}
    types = []
    segments = []
    exchanges = []
    ticksizes = []
    indices = []

    for ins in allins:
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
        if index['name'] == 'NIFTY BANK':
            # print(index)
            banknifty_instrument = index
            break
    return curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, banknifty_instrument


def find_trading_sessions(timestamp, expiry):
    t1 = time.time()
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = expopt * 25 - (
            timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300
    return trading_sessions
    # print(time.time() - t1, 'find_trading_sessions')


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


vixnow = 0

def find_vix(timestamp):
    closest_timestamp_fut = vixtd['date'].iloc[(vixtd['date']<timestamp).sum()]
    closest_timestamp_past = vixtd['date'].iloc[(vixtd['date'] < timestamp).sum()-1]
    if (closest_timestamp_fut - timestamp).seconds <= 360:
        return vixtd['open'].iloc[(vixtd['date']<timestamp).sum()]
    elif (timestamp - closest_timestamp_fut).seconds < 360:
        return vixtd['close'].iloc[(vixtd['date'] < timestamp).sum()-1]
    else:
        return 0

def find_spot(timestamp):
    closest_timestamp_fut = bankniftytd['date'].iloc[(bankniftytd['date']<timestamp).sum()]
    closest_timestamp_past = bankniftytd['date'].iloc[(bankniftytd['date'] < timestamp).sum()-1]
    if (closest_timestamp_fut - timestamp).seconds <= 360:
        return bankniftytd['open'].iloc[(bankniftytd['date']<timestamp).sum()]
    elif (timestamp - closest_timestamp_fut).seconds < 360:
        return bankniftytd['close'].iloc[(bankniftytd['date'] < timestamp).sum()-1]
    else:
        return 0

def find_closest_ltp_and_oi(timestamp, input_csv):
    try:
        ind = (input_csv['date']<=timestamp).sum()
        closest_timestamp_fut = input_csv['date'].iloc[ind]

        if (closest_timestamp_fut - timestamp).seconds <= 360:
            return input_csv['open'].iloc[(input_csv['date']<timestamp).sum()],input_csv['oi'].iloc[(input_csv['date']<timestamp).sum()]
        elif not ind:
            closest_timestamp_past = input_csv['date'].iloc[ind - 1]
            if (timestamp - closest_timestamp_fut).seconds < 360:
                return input_csv['close'].iloc[(input_csv['date'] < timestamp).sum()-1],input_csv['oi'].iloc[(input_csv['date']<timestamp).sum()-1]
        else:
            return 0
    except:
        return 0
def get_filtered_oi_and_ltps(timestamp):
    global strike_range, csv_database
    foldername = find_expiry(timestamp).strftime('%Y-%m-%d')
    path  = options_historical_dir + foldername + '/'
    instrumentlist = os.listdir(path)
    spot = find_spot(timestamp)
    lower_limit = spot - spot*prob_range
    upper_limit = spot + spot*prob_range
    filtered = []
    instrumentlist = [item.replace('.csv','') for item in instrumentlist]
    oi = {}
    opt_ltps = {}
    for item in instrumentlist:
        if lower_limit<name_to_strike(item)<upper_limit:
            filtered.append(item+'.csv')
    for item in filtered:
        key = item.replace('.csv','')
        if path + item not in csv_database.keys():
            df = pd.read_csv(path + item)
            # print(len(df))
            if len(df)-1:
                if len(df.columns)>7:
                    df = df.filter(['0','1','2', '3', '4', '5', '6'])
                csv_database[path+item] = df
                csv_database[path+item].columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'oi']
                dates = [datetime.datetime.strptime(item,'%d.%m.%y %H:%M:%S') for item in csv_database[path+item]['date'].to_list()]
                csv_database[path+item]['date'] = pd.Series(dates)
                res = find_closest_ltp_and_oi(timestamp, csv_database[path + item])
                if res:
                    opt_ltps[key], oi[key] = res
        else:
            res = find_closest_ltp_and_oi(timestamp, csv_database[path + item])
            if res:
                opt_ltps[key], oi[key] = res
    return opt_ltps,oi
def get_oi_and_ltps(timestamp):
    global strike_range, csv_database
    foldername = find_expiry(timestamp).strftime('%Y-%m-%d')
    path  = options_historical_dir + foldername + '/'
    instrumentlist = os.listdir(path)
    spot = find_spot(timestamp)
    lower_limit = spot - spot*strike_range
    upper_limit = spot + spot*strike_range
    filtered = []
    instrumentlist = [item.replace('.csv','') for item in instrumentlist]
    oi = {}
    opt_ltps = {}
    for item in instrumentlist:
        if lower_limit<name_to_strike(item)<upper_limit:
            filtered.append(item+'.csv')
    for item in filtered:
        key = item.replace('.csv','')
        if path + item not in csv_database.keys():
            df = pd.read_csv(path + item)
            # print(len(df))
            if len(df)-1:
                if len(df.columns)>7:
                    df = df.filter(['0','1','2', '3', '4', '5', '6'])
                csv_database[path+item] = df
                csv_database[path+item].columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'oi']
                dates = [datetime.datetime.strptime(item,'%d.%m.%y %H:%M:%S') for item in csv_database[path+item]['date'].to_list()]
                csv_database[path+item]['date'] = pd.Series(dates)
                res = find_closest_ltp_and_oi(timestamp, csv_database[path + item])
                if res:
                    opt_ltps[key], oi[key] = res
        else:
            res = find_closest_ltp_and_oi(timestamp, csv_database[path + item])
            if res:
                opt_ltps[key], oi[key] = res
    return opt_ltps,oi


def probability_below(timestamp):
    t1 = time.time()
    vixnow = find_vix(timestamp)
    if not vixnow:
        return
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    expiry = find_expiry(timestamp)
    trading_sessions = find_trading_sessions(timestamp, expiry)
    try:
        ndis = nifty_distribution(vix_min, vix_max, trading_sessions)
        data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
        sd = statistics.pstdev(ndis)
        mean = ndis.mean()
    except:
        ndis = nifty_distribution(0, 100, trading_sessions)
        data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
        sd = statistics.pstdev(ndis)
        mean = ndis.mean()
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    opt_ltps, oi = get_filtered_oi_and_ltps(timestamp)
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
    print(time.time() - t1, 'probability_below')
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


def copy_and_sort_spreads(put_spreads, call_spreads):
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
    # print(time.time() - t1, 'copy_and_sort_spreads')
    return put_spreads, call_spreads


def initiate_variables():
    if 'ledger_bn' in os.listdir(aux_files):
        ledger = pickle.load(open(aux_files + 'ledger_bn', 'rb'))
    else:
        ledger = pd.DataFrame()

    if 'open_positions_bn' in os.listdir(aux_files):
        open_positions = pickle.load(open(aux_files + 'open_positions_bn', 'rb'))
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
    # print(time.time() - t1, 'find_trades')
    return tradelist


def find_expiry(timestamp):
    t1 = time.time()
    for item in os.listdir(options_historical_dir):
        exp_opt_date = datetime.datetime.strptime(item,'%Y-%m-%d')

        if  exp_opt_date.date() >= timestamp.date():
            # print(time.time() - t1, 'find_expiry')
            return exp_opt_date.date()



def show_tradelist(ledger, open_positions, tradelist, timestamp, option_ltps, xl_put_spreads, xl_call_spreads):
    t1 = time.time()
    expiry = find_expiry(timestamp)
    nrlist = []
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = option_ltps[key]*1.01
            typ = 'buy'
        else:
            price = option_ltps[key]*0.99
            typ = 'sell'
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * 25, 'price': price,
              'oid': time.time()}
        nrlist.append(nr)
    time.sleep(1)
    for nr in nrlist:
        ledger = ledger.append(nr, ignore_index=True)
        key = nr['instrument']
        if key in open_positions.keys():
            open_positions[key] += tradelist[key] * 25
        else:
            open_positions[key] = tradelist[key] * 25
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
    # print(time.time() - t1, 'show_tradelist')
    return ledger, open_positions


def show_tradelist_sq(ledger, open_positions, tradelist, timestamp, option_ltps, xl_put_spreads, xl_call_spreads,spot):
    t1 = time.time()
    expiry = find_expiry(timestamp)
    nrlist = []
    expiry_price = {}
    for item in open_positions.keys():
        lastprice = 0
        if 'CE' in item:
            strike = name_to_strike(item)
            lastprice += max(0,spot-strike)
        if 'PE' in item:
            strike = name_to_strike(item)
            lastprice += max(0, strike - spot)
        expiry_price[item] = round_down(lastprice)
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = expiry_price[key]
            typ = 'buy'
        else:
            price = expiry_price[key]
            typ = 'sell'
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * 25, 'price': price,
              'oid': time.time()}
        nrlist.append(nr)
    time.sleep(1)
    for nr in nrlist:
        ledger = ledger.append(nr, ignore_index=True)
        key = nr['instrument']
        if key in open_positions.keys():
            open_positions[key] += tradelist[key] * 25
        else:
            open_positions[key] = tradelist[key] * 25
    poplist = []
    for key in open_positions.keys():
        if open_positions[key] == 0:
            poplist.append(key)
    for key in poplist:
        open_positions.pop(key)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    sqpnl = 0
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

    openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values()))
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
    # print(time.time() - t1, 'show_tradelist')
    return ledger, open_positions


def show_last_n_candles(timestamp,n):
    ind = (bankniftytd['date'] < timestamp).sum()
    closest_timestamp_fut = bankniftytd['date'].iloc[ind]
    if (closest_timestamp_fut - timestamp).seconds <= 360:
        cts = closest_timestamp_fut
    elif not ind:
        closest_timestamp_past = bankniftytd['date'].iloc[ind - 1]
        if (timestamp - closest_timestamp_fut).seconds < 360:
            cts = closest_timestamp_past
    till_ind = bankniftytd['date'][bankniftytd['date'] == cts].index[0]
    from_ind = till_ind - n
    df = bankniftytd.iloc[from_ind:till_ind]
    s.range('K50').value = df


def filter_for_tradable(put_spreads,call_spreads,timestamp):
    global trade_range, open_positions
    open_call_spread = []
    open_put_spread = []
    if len(open_positions):
        for key in open_positions.keys():
            if 'CE' in key:
                open_call_spread.append(key)
            if 'PE' in key:
                open_put_spread.append(key)
        open_call_spread.append(open_call_spread[0] + '-' + open_call_spread[1])
        open_call_spread.append(open_call_spread[1] + '-' + open_call_spread[0])
        open_put_spread.append(open_put_spread[0] + '-' + open_put_spread[1])
        open_put_spread.append(open_put_spread[1] + '-' + open_put_spread[0])
    spot = find_spot(timestamp)
    ll,ul = spot*(1-trade_range),spot*(1+trade_range)
    ret_put_spreads = {}
    ret_call_spreads = {}
    for item in put_spreads.keys():
        [s1,s2] = item.split('-')
        if ll<name_to_strike(s1)<ul and ll<name_to_strike(s2)<ul:
            ret_put_spreads[item] = put_spreads[item]
        if item in open_put_spread:
            ret_put_spreads[item] = put_spreads[item]
    for item in call_spreads.keys():
        [s1,s2] = item.split('-')
        if ll<name_to_strike(s1)<ul and ll<name_to_strike(s2)<ul:
            ret_call_spreads[item] = call_spreads[item]
        if item in open_call_spread:
            ret_call_spreads[item] = call_spreads[item]
    return ret_put_spreads,ret_call_spreads


def square_off_all():
    global open_positions
    tradelist = {}
    for key in open_positions.keys():
        tradelist[key] = -open_positions[key]/25
    return tradelist


def find_existing_positions_result(pos,price, expiry):
    name = list(pos.keys())[0]
    pnl = 0
    if 'CE' in name:
        strike = name_to_strike(name)
        diff = expiry - strike
        if diff <= 0:
            pnl = pnl - (price * pos[name])
        else:
            if pos[name] > 0:
                pnl = pnl + ((expiry - strike - price) * pos[name])
            else:
                pnl = pnl - ((price - expiry + strike) * pos[name])
    elif 'PE' in pos:
        strike = name_to_strike(name)
        diff = expiry - strike
        if diff >= 0:
            pnl = pnl - (price * pos[name])
        else:
            if pos[name] > 0:
                pnl = pnl + ((strike - expiry - price) * pos[name])
            else:
                pnl = pnl - ((expiry - strike + price) * pos[name])
    return pnl


def payoff_graph(open_positions, opt_ltps):
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
    for spot in pnl.keys():
        resultdf = resultdf.append({'spot':spot,'positions':pnl[spot]},ignore_index=True)
    s.range('K34').value = resultdf
    # figure = resultdf.plot(x='spot', y='positions', grid=1, label='PnL',).get_figure()
    # s.pictures.add(figure, name='Payoff Graph', update=True,left=s.range('B5').left, top=s.range('B5').top)
    # opt_ltps = {}
    # for key in open_positions.keys():
    #     opt_ltps[key] = (name_to_strike(key)/100)*(name_to_strike(key)/100)





curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, banknifty_instrument = make_curr_ins()
nifty50list = pd.read_csv('ind_nifty50list.csv')['Symbol'].to_list()
print('Getting Historical Data')
vix = zd.get_historical(vix_instrument['instrument_token'], fdate.date(), tdate.date(), "5minute", 0)
vixtd = zd.get_historical(vix_instrument['instrument_token'], fdate.date(), datetime.datetime.now().date(), "5minute", 0)
banknifty = zd.get_historical(banknifty_instrument['instrument_token'], fdate.date(), tdate.date(), "5minute", 0)
bankniftytd = zd.get_historical(banknifty_instrument['instrument_token'], fdate.date(), datetime.datetime.now().date(), "5minute", 0)
vix = vix.loc[vix['date'].isin(banknifty['date'])]
banknifty = banknifty.loc[banknifty['date'].isin(vix['date'])]

opt_ltps = {}
ledger, open_positions = initiate_variables()

s.range('J1').value = 1
print('Initiating')
trading_days = os.listdir('dayoptionticks')

simulate_start = datetime.datetime(2020,1,1,9,15,0)
simulate_end = datetime.datetime(2021,4,29,15,15,0)
date_range = bankniftytd['date'][bankniftytd['date'] > simulate_start]
date_range = date_range[date_range < simulate_end]
if len(ledger):
    last_trade = ledger['timestamp'].iloc[-1]
    date_range = date_range[date_range > last_trade]
for timestamp in date_range:
    print(timestamp)
    show_last_n_candles(timestamp,45)
    if s.range('J1').value == 0:
        break
    try:
        opt_ltps,oi = get_oi_and_ltps(timestamp)
    except:
        print(timestamp, 'Error in getting LTP')
        if timestamp.date() == find_expiry(timestamp) and timestamp.time() >= datetime.time(15, 20, 0):
            pass
        else:
            continue

    try:
        if len(open_positions):
            payoff_graph(open_positions, opt_ltps)
    except:
        pass
    try:
        if timestamp.date() == find_expiry(timestamp) and timestamp.time() >= datetime.time(15,20,0):
            if len(open_positions):
                tradelist = square_off_all()
                spot = float(bankniftytd['close'][bankniftytd['date'] == timestamp])
                ledger, open_positions = show_tradelist_sq(ledger, open_positions, tradelist, timestamp, opt_ltps,
                                                        xl_put_spreads, xl_call_spreads,spot)
                csv_database = {}
            else:
                continue

        if timestamp.time() > market_open:
            try:
                prob_below, opt_ltps = probability_below(timestamp)
            except TypeError:
                print('Error probability_below')
                continue
            if len(prob_below):
                try:
                    put_spreads, call_spreads = spread_evs(prob_below, opt_ltps)
                except:
                    print('Error spread_evs')
                    continue
            else:
                continue


            try:
                put_spreads,call_spreads = filter_for_tradable(put_spreads,call_spreads,timestamp)
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
                put_spreads, call_spreads = copy_and_sort_spreads(put_spreads, call_spreads)
            except:
                print('Error 3')
            try:
                tradelist = find_trades(open_positions, put_spreads, call_spreads)
            except:
                print('Error 4')
            try:
                ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp, opt_ltps,xl_put_spreads, xl_call_spreads)
            except:
                print('Error 5')
                continue


            if len(tradelist) > 0:
                # pingdiscord(str(tradelist))
                s.range('K34:M45').clear()
                payoff_graph(open_positions, opt_ltps)
                pickle.dump(open_positions, open(aux_files + 'open_positions_bn', 'wb'))
                pickle.dump(ledger, open(aux_files + 'ledger_bn', 'wb'))
    except Exception as e:
        print(e)
        pass


