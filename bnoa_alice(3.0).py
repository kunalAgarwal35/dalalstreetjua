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
import alice_orders as ao
from nsetools import Nse

file_path = 'live.xlsx'
book = xw.Book(file_path)
s = book.sheets['banknifty']
s.clear_contents()
market_open = datetime.time(9, 16, 00)
market_close = datetime.time(15, 30, 00)
allins = zd.kite.instruments()

iupac = "%m%d%Y-%H%M%S-%f"
iupac = "%m%d%Y-%H%M%S-%f"
option_tickdir = 'optionticks/'
aux_files = 'optionadjust/'
wbhk = 'https://discordapp.com/api/webhooks/833393245870489631/sMqbM-uxq5ojp8tK8Wt_oHN4b0GNe6Rggk_PgS1UVbb1TtvQOY_mkLfOFCozvEjdydnW'
# Parameters:
# VIX range % (up and down)
vix_range_percent = .2
# VIX and NIFTY historical sample range
fdate = datetime.datetime(2011, 1, 1)
tdate = datetime.datetime(2021,7,23)
# When to adjust positions
percentile = 85
# Frequency of processing (seconds, upto 60)
freq = 15
# % of spot up and down to limit trading strikes
trade_range = 0.03
prob_range = 0.08
dayformat="%y-%m-%d"
index_name = 'NIFTY BANK'
# index_name = 'NIFTY 50'
# aux_name = 'nifty50'
insname = 'BANKNIFTY'
# insname = 'NIFTY'
aux_name = 'banknifty'
# aux_name = 'nifty50'
def pingdiscord(txt):
    wbhook = DiscordWebhook(url=wbhk, content=txt)
    response = wbhook.execute()


def tradable_strikes():
    global trade_range
    nse = Nse()
    # print(nse)
    # index_codes = nse.get_index_list()
    q = nse.get_index_quote('nifty bank')
    spot = q['lastPrice']
    try:
        return [round_down(spot*(1-trade_range)),round_down(spot*(1+trade_range))]
    except Exception as e:
        print(e, 'Could return tradable_strikes')


def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


def openposnames():
    global open_positions
    calls = {}
    puts = {}
    for item in open_positions.keys():
        if 'CE' in item:
            calls[item] = open_positions[item]
        if 'PE' in item:
            puts[item] = open_positions[item]
    calls = dict(sorted(calls.items(),reverse=True))
    puts = dict(sorted(puts.items()))
    callspr = ('-').join(list(calls.keys()))
    putspr = ('-').join(list(puts.keys()))
    s.range('J4').value = callspr
    s.range('J5').value = putspr

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
    expiry = find_expiry()

    for ins in allins:
        curr_ins[ins['instrument_token']] = ins
    for ins in curr_ins.keys(
    ):
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
    for key in curr_ins.keys():
        ts[curr_ins[key]['tradingsymbol']] = curr_ins[key]['instrument_token']
        if curr_ins[key]['instrument_type'] in ['CE', 'PE'] and curr_ins[key]['name'] == insname and curr_ins[key]['expiry'] == expiry:
            engts[str(int(curr_ins[key]['strike'])) + curr_ins[key]['instrument_type']] = curr_ins[key]['tradingsymbol']

    return curr_ins, types, segments, exchanges, ticksizes, indices, vix_instrument, nifty_50_instrument, engts, ts

def find_trading_sessions(timestamp, expiry):
    t1 = time.time()
    expopt = np.busday_count(timestamp.date(), expiry) + 1
    trading_sessions = expopt * 75 - (
            timestamp - datetime.datetime.combine(timestamp.date(), datetime.time(9, 15, 00))).seconds / 300
    return trading_sessions - 30


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))

def pnlgraph(timestamp,openp,booked,net,slippage):
    global pnldf
    pnldf = pnldf.append({'timestamp':timestamp, 'Open P/L':openp,'Booked P/L':booked,'Slippage':slippage,'Net':(net - slippage)},ignore_index = True)
    pickle.dump(pnldf,open(aux_files+'pnldf_'+aux_name,'wb'))
    return pnldf


vixnow = 0

def display_sdandmean(timestamp):
    global vixnow
    try:
        vix_min = (1 - vix_range_percent) * vixnow
        vix_max = (1 + vix_range_percent) * vixnow
        expiry = find_expiry()
        trading_sessions = find_trading_sessions(timestamp, expiry)
        ndis = nifty_distribution(vix_min, vix_max, trading_sessions)
        data = np.arange(round_down(min(ndis)), round_down(max(ndis)), 0.01)
        sd = statistics.pstdev(ndis)
        mean = ndis.mean()
        s.range('L6').value = {'Mean':mean,'SD':sd}
    except Exception as e:
        s.range('L6').value = e
        return
    return

def probability_below(timestamp, opt_ltps,opt_ois):
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
    global open_positions
    open_call_spread,ctemp = [],[]
    open_put_spread,ptemp = [],[]
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
    [l, u] = tradable_strikes()
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
    return ret_put, ret_call

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
    return put_spreads, call_spreads

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

def payoff_graph(open_positions, opt_ltps):
    global spot
    for item in open_positions.keys():
        if item not in opt_ltps.keys():
            return
    s.range('I9').value = {'spot':spot}
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
    # figure = resultdf.plot(x='spot', y='positions', grid=1, label='PnL',).get_figure()
    # s.pictures.add(figure, name='Payoff Graph', update=True,left=s.range('B5').left, top=s.range('B5').top)
    # opt_ltps = {}
    # for key in open_positions.keys():
    #     opt_ltps[key] = (name_to_strike(key)/100)*(name_to_strike(key)/100)

def initiate_variables():
    if 'ledger_'+aux_name in os.listdir(aux_files):
        ledger = pickle.load(open(aux_files + 'ledger_'+aux_name, 'rb'))
    else:
        ledger = pd.DataFrame(columns = ['expiry','instrument','oid','price','qty','value'])

    if 'open_positions_'+aux_name in os.listdir(aux_files):
        open_positions = pickle.load(open(aux_files + 'open_positions_'+aux_name, 'rb'))
    else:
        open_positions = {}
    if 'pnldf_'+aux_name in os.listdir(aux_files):
        pnldf = pickle.load(open(aux_files + 'pnldf_'+aux_name, 'rb'))
    else:
        pnldf = pd.DataFrame()
    return ledger, open_positions,pnldf

def find_trades(open_positions, put_spreads, call_spreads):
    global percentile
    percentile = s.range('J8').value
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
        s.range('I6').value = {'Call EV Thresh:':call_spread_ev_thresh,'Put EV Thresh:':put_spread_ev_thresh}
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

def find_expiry():
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100
    for ins in allins:
        if ins['name'] == insname and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days
                exp_opt_date = ins['expiry']
    return exp_opt_date

def show_tradelist(ledger, open_positions, tradelist, timestamp, option_ltps, xl_put_spreads, xl_call_spreads, ts):
    expiry = find_expiry()
    nrlist = []
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = option_ltps[key]
            typ = 'buy'
        else:
            price = option_ltps[key]
            typ = 'sell'
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * curr_ins[ts[engts[key]]]['lot_size'], 'price': price,
              'oid': ''}
        nrlist.append(nr)
    time.sleep(1)
    oh = ao.process_banknifty_tradelist(tradelist)
    for nr in nrlist:
        nr['status'] = oh[nr['instrument']]['status']
        nr['average_price'] = oh[nr['instrument']]['avg_price']
        nr['exchange_timestamp'] =  oh[nr['instrument']]['timestamp']
        # nr['tradingsymbol'] = oh[nr['instrument']]['tradingsymbol']
        ledger = ledger.append(nr, ignore_index=True)
        key = nr['instrument']
        if nr['status'] == 'complete':
            if key in open_positions.keys():
                open_positions[key] += tradelist[key] * curr_ins[ts[engts[key]]]['lot_size']
            else:
                open_positions[key] = tradelist[key] * curr_ins[ts[engts[key]]]['lot_size']
    if len(nrlist):
        disout = [str(nr)+'\n' for nr in nrlist]
        pingdiscord(disout)
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
        if key not in option_ltps.keys():
            print(key)
            sqpnl = 0
            break
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
    try:
        print_ledger['slippage'] = print_ledger['price'] - print_ledger['average_price']
        print_ledger['slippage'] = print_ledger['slippage'] * print_ledger['qty']
    except:
        print_ledger['slippage'] = 0
        pass
    openposnames()
    if sqpnl:
        openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
        netpnl = print_ledger['value'].sum() + sqpnl
        slipped = print_ledger['slippage'].sum()
        pnldict = {'Open P/L': openpnl, 'Booked P/L': sum(booked_pnl.values()),
                   'Net P/L': netpnl, 'Slippage': slipped}
        eq_curve = pnlgraph(timestamp, openpnl, sum(booked_pnl.values()), netpnl, slipped)
        # s.range('AE1').value = eq_curve
        s.range('F6').value = pnldict
        s.range('F16').value = booked_pnl

    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    s.range('A1:D500').clear_contents()
    s.range('A1').value = xl_put_spreads
    s.range('C1').value = xl_call_spreads

    # book.app.screen_updating = True
    return ledger, open_positions

def update_ltps(last_options_tick, opt_ltps,opt_ois):
    global spot,prob_range,nifty_50_instrument
    if nifty_50_instrument['instrument_token'] in last_options_tick.keys():
        spot = last_options_tick[nifty_50_instrument['instrument_token']]['last_price']
    else:
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

def square_off_all():
    global open_positions
    tradelist = {}
    for key in open_positions.keys():
        tradelist[key] = -open_positions[key]
    return tradelist

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
ledger, open_positions,pnldf = initiate_variables()
last_processed = ''
s.range('J1').value = 1
s.range('I8').value = 'Percentile'
s.range('J8').value = percentile
print('Initiating')
while datetime.datetime.now().time() < market_close and s.range('J1').value:
    if market_open > datetime.datetime.now().time():
        time.sleep(5)
        continue
    fname = os.listdir(option_tickdir)[-1]
    timestamp = datetime.datetime.strptime(fname, iupac)
    # if not timestamp.second % 58:
    #     opt_ltps,opt_ois = {},{}
    if timestamp == last_processed:
        time.sleep(0.5)
        continue
    else:
        last_processed = timestamp
    print(timestamp)
    try:
        last_options_tick = pickle.load(open(option_tickdir + fname, 'rb'))
    except EOFError:
        time.sleep(0.2)
        try:
            last_options_tick = pickle.load(open(option_tickdir + fname, 'rb'))
        except:
            continue
    opt_ltps, opt_ois = update_ltps(last_options_tick, opt_ltps,opt_ois)

    if len(open_positions):
        payoff_graph(open_positions,opt_ltps)

    if timestamp.date() == find_expiry() and timestamp.time() > datetime.time(15,10,0):
        if len(open_positions):
            tradelist = square_off_all()
            ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp, opt_ltps,
                                                    xl_put_spreads, xl_call_spreads, ts)
        else:
            continue
    if vix_instrument['instrument_token'] in last_options_tick.keys():
        vixnow = last_options_tick[vix_instrument['instrument_token']]['last_price']
    if nifty_50_instrument['instrument_token'] in last_options_tick.keys():
        spot = last_options_tick[nifty_50_instrument['instrument_token']]['last_price']
    if (not vixnow) or not(spot):
        print('Waiting for Spot and Vix')
        continue
    if not timestamp.second % freq:
        prob_below = probability_below(timestamp, opt_ltps, opt_ois)
        display_sdandmean(timestamp)

        if len(prob_below):
            put_spreads, call_spreads = spread_evs(prob_below, opt_ltps)
            put_spreads, call_spreads = filter_for_tradable_spreads(put_spreads, call_spreads)
        else:
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

            put_spreads, call_spreads = copy_and_sort_spreads(put_spreads, call_spreads)
            tradelist = find_trades(open_positions, put_spreads, call_spreads)

        if len(tradelist):
            if len(ol_tradelist):
                if ol_tradelist == tradelist:
                    ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp,
                                                            opt_ltps,
                                                            xl_put_spreads, xl_call_spreads, ts)
                    pickle.dump(open_positions, open(aux_files + 'open_positions_'+aux_name, 'wb'))
                    pickle.dump(ledger, open(aux_files + 'ledger_'+aux_name, 'wb'))
                    ol_tradelist = {}
                else:
                    print("Tradelist Rejected")
                    ol_tradelist = tradelist

            else:
                ol_tradelist = tradelist

        else:
            _ = 1
            for key in open_positions.keys():
                if key not in opt_ltps:
                    _ = 0
                    break
            if _:
                ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp, opt_ltps,
                                                        xl_put_spreads, xl_call_spreads, ts)
                opt_ltps, opt_ois = {}, {}

    else:
        try:
            show_tradelist(ledger, open_positions, {}, timestamp, opt_ltps, xl_put_spreads, xl_call_spreads, ts)
        except:
            try:
                show_tradelist(ledger, open_positions, {}, timestamp, opt_ltps, [], [], ts)
            except:
                print('Error 7')
                pass



