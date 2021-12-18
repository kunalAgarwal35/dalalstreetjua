import opt_chain_probmodel as opc
import datetime
import math
import os
import pickle
import statistics
import time
import numpy as np
import pandas as pd
import xlwings as xw
from nsetools import Nse
from scipy.stats import norm
import function_store2 as fs2



file_path = 'C:/Kiteconnect/xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['nifty_backtest']
s.clear_contents()
market_open = datetime.time(9, 16, 00)
market_close = datetime.time(15, 30, 00)
nlots = 1
aux_files = 'optionadjust/'
# Parameters:
# VIX range % (up and down)
# VIX and NIFTY historical sample range
fdate = datetime.datetime(2011, 1, 1)
tdate = datetime.datetime(2021, 10, 26)
# When to adjust positions
percentile = 80
# Frequency of processing (seconds, upto 60)
freq = 15
# % of spot up and down to limit trading strikes
trade_range = 0.04
prob_range = 0.05
aux_name = 'nifty_backtest'

def tradable_strikes():
    global trade_range
    nse = Nse()
    # print(nse)
    # index_codes = nse.get_index_list()
    q = nse.get_index_quote('nifty bank')
    spot = q['lastPrice']
    try:
        return [round_down(spot * (1 - trade_range)), round_down(spot * (1 + trade_range))]
    except Exception as e:
        print(e, 'Couldnt return tradable_strikes')


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
    calls = dict(sorted(calls.items(), reverse=True))
    puts = dict(sorted(puts.items()))
    callspr = '-'.join(list(calls.keys()))
    putspr = '-'.join(list(puts.keys()))
    s.range('J4').value = callspr
    s.range('J5').value = putspr


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


def pnlgraph(timestamp, openp, booked, net, slippage):
    global pnldf
    pnldf = pnldf.append({'timestamp': timestamp, 'Open P/L': openp, 'Booked P/L': booked, 'Slippage': slippage,
                          'Net': (net - slippage)}, ignore_index=True)
    pickle.dump(pnldf, open(aux_files + 'pnldf_' + aux_name, 'wb'))
    return pnldf


def copy_and_sort_spreads(put_spreads, call_spreads):
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
        put_spreads[key] = round_down(put_spreads[key])
    for key in call_spreads.keys():
        call_spreads[key] = round_down(call_spreads[key])
    return put_spreads, call_spreads


def find_existing_positions_result(pos, price, expiry):
    name = list(pos.keys())[0]
    pnl = 0
    strike = name_to_strike(name)
    diff = abs(expiry - strike)
    if 'CE' in name:
        if expiry > strike:
            return (diff - price) * pos[name]
        else:
            return -price * pos[name]
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
    s.range('I9').value = {'spot': spot}
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
            pnl[expiry] += find_existing_positions_result({key: calls[key]}, opt_ltps[key], expiry)
        for key in puts.keys():
            pnl[expiry] += find_existing_positions_result({key: puts[key]}, opt_ltps[key], expiry)

    resultdf = pd.DataFrame(columns=['spot', 'positions'])
    for sp in pnl.keys():
        resultdf = resultdf.append({'spot': sp, 'positions': pnl[sp]}, ignore_index=True)
    s.range('I17:K26').clear()
    s.range('I17').value = resultdf
    # figure = resultdf.plot(x='spot', y='positions', grid=1, label='PnL',).get_figure()
    # s.pictures.add(figure, name='Payoff Graph', update=True,left=s.range('B5').left, top=s.range('B5').top)
    # opt_ltps = {}
    # for key in open_positions.keys():
    #     opt_ltps[key] = (name_to_strike(key)/100)*(name_to_strike(key)/100)


def initiate_variables():
    ledger = pd.DataFrame(columns=['expiry', 'instrument', 'oid', 'price', 'qty', 'value'])
    open_positions = {}
    pnldf = pd.DataFrame()

    return ledger, open_positions, pnldf

def transaction_cost_options(buy_price,sell_price, qty):
    # Zerodha Transaction Cost Options
    brokerage = 20
    # stt is on sell side (on premium)
    stt = 0.0005
    transaction_charges = 0.00053
    # gst is on brokerage + transaction charges
    gst = 0.18
    sebi_charges_per_10mil = 10
    stamp_charges_per_10mil = 300
    turnover = qty * (buy_price + sell_price)
    brokerage = brokerage * 2
    stt_total = sell_price * qty * stt
    exchange_txn_charge = turnover * transaction_charges
    tot_gst = (brokerage + exchange_txn_charge) * gst
    sebi_charges = turnover/10000000 * sebi_charges_per_10mil
    stamp_duty = buy_price*qty/10000000 * stamp_charges_per_10mil
    total_cost = brokerage + stt_total + exchange_txn_charge + tot_gst + sebi_charges + stamp_duty
    return total_cost







def find_trades(open_positions, put_spreads, call_spreads):
    global percentile,nlots
    percentile = s.range('J8').value
    tradelist = {}
    if len(open_positions) == 0:
        for key in call_spreads.keys():
            if call_spreads[key] == max(call_spreads.values()):
                s1 = key[:key.index('-')]
                s2 = key[key.index('-') + 1:]
                tradelist[s1] = 1*nlots
                tradelist[s2] = -1*nlots
        for key in put_spreads.keys():
            if put_spreads[key] == max(put_spreads.values()):
                s1 = key[:key.index('-')]
                s2 = key[key.index('-') + 1:]
                tradelist[s1] = 1*nlots
                tradelist[s2] = -1*nlots
    else:
        call_spread_ev_thresh = np.percentile(pd.Series(call_spreads.values()), percentile)
        put_spread_ev_thresh = np.percentile(pd.Series(put_spreads.values()), percentile)
        cspr = {}
        pspr = {}
        s.range('I6').value = {'Call EV Thresh:': call_spread_ev_thresh, 'Put EV Thresh:': put_spread_ev_thresh}
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
                tradelist[cspr[0]] = -1*nlots
                tradelist[cspr[1]] = 1*nlots
                for key in call_spreads.keys():
                    if call_spreads[key] == max(call_spreads.values()):
                        s1 = key[:key.index('-')]
                        s2 = key[key.index('-') + 1:]
                        if s1 not in list(tradelist.keys()):
                            tradelist[s1] = 1*nlots
                        else:
                            tradelist[s1] += 1*nlots
                        if s2 not in list(tradelist.keys()):
                            tradelist[s2] = -1*nlots
                        else:
                            tradelist[s2] += -1*nlots
                        break
        except KeyError:
            pass
        try:
            curputspr = pspr[0] + '-' + pspr[1]
            if put_spreads[curputspr] < put_spread_ev_thresh and max(put_spreads.values()) > 20:
                tradelist[pspr[0]] = -1*nlots
                tradelist[pspr[1]] = 1*nlots
                for key in put_spreads.keys():
                    if put_spreads[key] == max(put_spreads.values()):
                        s1 = key[:key.index('-')]
                        s2 = key[key.index('-') + 1:]
                        if s1 not in list(tradelist.keys()):
                            tradelist[s1] = 1*nlots
                        else:
                            tradelist[s1] += 1*nlots
                        if s2 not in list(tradelist.keys()):
                            tradelist[s2] = -1*nlots
                        else:
                            tradelist[s2] += -1*nlots
                        break
        except KeyError:
            pass
    keys = list(tradelist.keys())
    for key in keys:
        if tradelist[key] == 0:
            tradelist.pop(key)
    # print(time.time() - t1, 'find_trades')
    return tradelist

def show_tradelist(ledger, open_positions, tradelist, timestamp, opt_ltps, xl_put_spreads, xl_call_spreads, expiry):
    global pnldf
    nrlist = []
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = opt_ltps[key]
        else:
            price = opt_ltps[key]
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * 50, 'price': price,
              'oid': ''}
        nrlist.append(nr)
    for nr in nrlist:
        nr['status'] = 'complete'
        nr['average_price'] = nr['price']
        nr['exchange_timestamp'] = timestamp
        ledger = ledger.append(nr, ignore_index=True)
        key = nr['instrument']
        if nr['status'] == 'complete':
            if key in open_positions.keys():
                open_positions[key] += tradelist[key] * 50
            else:
                open_positions[key] = tradelist[key] * 50

    for key in list(open_positions.keys()):
        if open_positions[key] == 0:
            open_positions.pop(key)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    sqpnl = 0
    for key in open_positions.keys():
        if key not in opt_ltps.keys():
            print(key)
            sqpnl = 0
            break
        sqpnl += opt_ltps[key] * open_positions[key]
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
        # s.range('AE1').value = eq_curve
        s.range('F6').value = pnldict
        s.range('F16').value = booked_pnl
    if 'netpnl' in locals():
        pnldf = pnldf.append({'timestamp': timestamp, 'Value': netpnl}, ignore_index=True)
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    s.range('A1:D500').clear_contents()
    s.range('A1').value = xl_put_spreads
    s.range('C1').value = xl_call_spreads
    s.range('M1').value = pnldf

    # book.app.screen_updating = True
    return ledger, open_positions


def square_off_all():
    global open_positions
    tradelist = {}
    for key in open_positions.keys():
        tradelist[key] = -open_positions[key]
    return tradelist


def adjust_bid_ask(putspreads, callspreads, bidaskspread):
    for spread in putspreads:
        contracts = spread.split('-')
        putspreads[spread] -= (bidaskspread[contracts[0]] + bidaskspread[contracts[1]]) / 2
    for spread in callspreads:
        contracts = spread.split('-')
        callspreads[spread] -= (bidaskspread[contracts[0]] + bidaskspread[contracts[1]]) / 2
    return putspreads, callspreads


print('Getting Historical Data')
vix = fs2.vix
nifty = fs2.nifty
vix = vix.loc[vix['date'].isin(nifty['date'])]
nifty = nifty.loc[nifty['date'].isin(vix['date'])]

opt_ltps = {}
opt_ois = {}
bidaskspread = {}
ol_tradelist = {}
spot = 0
ledger, open_positions, pnldf = initiate_variables()
last_processed = ''
s.range('J1').value = 1
s.range('I8').value = 'Percentile'
s.range('J8').value = percentile
print('Initiating')

expiry_dir = 'ev_backtest_data'
expiry_files = dict(zip(os.listdir(expiry_dir), [datetime.datetime.strptime(x.split('_')[1],'%d%b%Y').date() for x in os.listdir(expiry_dir)]))
expiry_files = dict(sorted(expiry_files.items(), key=lambda x: x[1]))
for file in list(expiry_files.keys()):
    pbdata = pickle.load(open(expiry_dir + '/' + file, 'rb'))
    print(file)
    expiry = expiry_files[file]
    for timestamp in list(pbdata.keys()):
        # if timestamp.minute % 5 != 0 or timestamp.second != 0:
        #     continue
        print(timestamp)
        opt_ltps, opt_ois, bidaskspread = pbdata[timestamp]['ltps'], pbdata[timestamp]['ois'], pbdata[timestamp]['bidasks']
        if len(open_positions):
            payoff_graph(open_positions, opt_ltps)

        if timestamp.date() == expiry and timestamp.time() >= datetime.time(15, 20, 0):
            if len(open_positions):
                for key in open_positions.keys():
                    spot = pbdata[timestamp]['spot']
                    if 'CE' in key:
                        if spot > name_to_strike(key):
                            opt_ltps[key] = spot - name_to_strike(key)
                        else:
                            opt_ltps[key] = 0
                    elif 'PE' in key:
                        if spot < name_to_strike(key):
                            opt_ltps[key] = name_to_strike(key) - spot
                        else:
                            opt_ltps[key] = 0
                tradelist = dict()
                for key in open_positions.keys():
                    tradelist[key] = -open_positions[key]/50
                print('squaring_off')
                print(tradelist,open_positions)
                ledger, open_positions = show_tradelist(ledger, open_positions,tradelist, timestamp,opt_ltps,
                                                        {}, {}, expiry)
                print('Squared off ', open_positions)
                continue
            else:
                continue
        else:
            # check if open positions are in ltps
            cont = False
            if len(open_positions):
                for key in open_positions.keys():
                    if key not in opt_ltps.keys():
                        cont = True
                        break
                if cont:
                    continue



        if not timestamp.second % freq:
            try:
                put_spreads, call_spreads = opc.get_evs(opt_ltps, bidaskspread, 0.1,0.05)
            except:
                print('Error in getting spreads')
                continue
            if not len(put_spreads) or not len(call_spreads):
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
                ledger, open_positions = show_tradelist(ledger, open_positions, tradelist, timestamp,
                                                        opt_ltps,
                                                        xl_put_spreads, xl_call_spreads, expiry)


