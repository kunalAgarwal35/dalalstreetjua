import pickle
import pandas as pd
import numpy as np
import function_store as fs
import zd
import datetime
import os
import time
import xlwings as xw
import statistics
file_path = 'xlwings.xlsx'
book = xw.Book(file_path)

# aux_name = 'ADANIPORTS'
# aux_name = 'ADANIENT'
# aux_name = 'AMBUJACEM'
# aux_name = 'APOLLOHOSP'
# aux_name = 'TATASTEEL'
aux_name = 'NIFTY'
s = book.sheets[aux_name]
s.clear_contents()

aux_files = 'optionadjustbacktest/'

parent_dir = 'nse_options_historical/'
symbols = pd.read_csv('nifty250.csv')['Symbol'].to_list()
final_dayformat = '%y-%m-%d'
percentile = 95
trade_range = 0.02
prob_range = 0.05
vix_range_percent = 0.2
sim_start_date = datetime.date(2018,1,1)
allins = zd.kite.instruments()
curr_ins = {}
for ins in allins:
    curr_ins[ins['instrument_token']] = ins

def get_symb_history(symb):
    fdate = datetime.datetime(2011,1,1)
    tdate = datetime.datetime(2021,7,30)
    for token in curr_ins.keys():
        if curr_ins[token]['tradingsymbol'] == symb:
            ohlc = zd.get_historical(token,fdate,tdate,'day',0)
            return ohlc
    if symb =='NIFTY':
        for token in curr_ins.keys():
            if curr_ins[token]['name'] == 'NIFTY 50' and curr_ins[token]['segment'] == 'INDICES':
                ohlc = zd.get_historical(token, fdate, tdate, 'day', 0)
                return ohlc
    if symb =='BANKNIFTY':
        for token in curr_ins.keys():
            if curr_ins[token]['name'] == 'NIFTY BANK' and curr_ins[token]['segment'] == 'INDICES':
                ohlc = zd.get_historical(token, fdate, tdate, 'day', 0)
                return ohlc
    if symb =='VIX':
        for token in curr_ins.keys():
            if curr_ins[token]['name'] == 'INDIA VIX' and curr_ins[token]['segment'] == 'INDICES':
                ohlc = zd.get_historical(token, fdate, tdate, 'day', 0)
                return ohlc

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
def pnlgraph(timestamp,openp,booked,net,slippage):
    global pnldf
    pnldf = pnldf.append({'timestamp':timestamp, 'Open P/L':openp,'Booked P/L':booked,'Slippage':slippage,'Net':(net - slippage)},ignore_index = True)
    pickle.dump(pnldf,open(aux_files+'pnldf_'+aux_name,'wb'))
    return pnldf
def show_tradelist_ohlc_backtest(ledger, open_positions, tradelist, timestamp, option_ltps, xl_put_spreads, xl_call_spreads, expiry,lot_size):
    nrlist = []
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = option_ltps[key]*1.02
            typ = 'buy'
        else:
            price = option_ltps[key]*0.98
            typ = 'sell'
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * lot_size, 'price': price,
              'oid': ''}
        nrlist.append(nr)
    # time.sleep(1)
    # oh = ao.process_banknifty_tradelist(tradelist)
    for nr in nrlist:
        # nr['status'] = oh[nr['instrument']]['status']
        # nr['average_price'] = oh[nr['instrument']]['avg_price']
        # nr['exchange_timestamp'] =  oh[nr['instrument']]['timestamp']
        nr['status'] = 'complete'
        nr['average_price'] = nr['price']
        nr['exchange_timestamp'] = nr['timestamp']
        # nr['tradingsymbol'] = oh[nr['instrument']]['tradingsymbol']
        ledger = ledger.append(nr, ignore_index=True)
        key = nr['instrument']
        if nr['status'] == 'complete':
            if key in open_positions.keys():
                open_positions[key] += tradelist[key] * lot_size
            else:
                open_positions[key] = tradelist[key] * lot_size
    if len(nrlist):
        disout = [str(nr)+'\n' for nr in nrlist]
        # pingdiscord(disout)
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
        print_ledger['slippage'] = print_ledger['slippage']*print_ledger['qty']
    except:
        print_ledger['slippage'] = 0
        pass
    openposnames()
    if sqpnl:
        openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
        netpnl = print_ledger['value'].sum() + sqpnl
        slipped = print_ledger['slippage'].sum()
        pnldict = {'Open P/L': openpnl, 'Booked P/L': sum(booked_pnl.values()),
                   'Net P/L': netpnl,'Slippage':slipped}
        eq_curve = pnlgraph(timestamp, openpnl, sum(booked_pnl.values()), netpnl, slipped)
        s.range('AE1').value = eq_curve
        s.range('F6').value = pnldict
        s.range('F16').value = booked_pnl

    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    s.range('A1:D500').clear_contents()
    s.range('A1').value = xl_put_spreads
    s.range('C1').value = xl_call_spreads

    # book.app.screen_updating = True
    return ledger, open_positions


def show_tradelist_sq(ledger, open_positions, tradelist, timestamp, option_ltps, xl_put_spreads, xl_call_spreads,expiry,lot_size,spot):
    t1 = time.time()
    nrlist = []
    expiry_price = {}
    for item in open_positions.keys():
        lastprice = 0
        if 'CE' in item:
            strike = fs.name_to_strike(item)
            lastprice += max(0,spot-strike)
        if 'PE' in item:
            strike = fs.name_to_strike(item)
            lastprice += max(0, strike - spot)
        expiry_price[item] = fs.round_down(lastprice)
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

def find_lotsize(symb):
    for ins in curr_ins.keys():
        if curr_ins[ins]['name'] == symb and curr_ins[ins]['instrument_type'] == 'FUT':
            return curr_ins[ins]['lot_size']

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

def get_stats_from_vix(timestamp,vix_range_percent,vixnow,expiry,vix,nifty):
    mean,sd = 0,0
    trading_sessions = np.busday_count(timestamp, expiry) + 1
    vix_min = (1 - vix_range_percent) * vixnow
    vix_max = (1 + vix_range_percent) * vixnow
    ndis = nifty_distribution_custom(vix_min, vix_max, trading_sessions,vix,nifty)
    data = np.arange(fs.round_down(min(ndis)), fs.round_down(max(ndis)), 0.01)
    sd = statistics.pstdev(ndis)
    mean = ndis.mean()
    return mean,sd


symb = aux_name
expiry_dir = parent_dir+symb+'/'
symbhistorical = get_symb_history(symb)
vixhistorical = get_symb_history('VIX')
symbhistorical['date'] = pd.to_datetime(symbhistorical['date'])
symbhistorical['date'] = symbhistorical['date'].dt.date
vixhistorical['date'] = pd.to_datetime(vixhistorical['date'])
vixhistorical['date'] = vixhistorical['date'].dt.date

database = {}
#pass 0 in initiate variables to start from scratch
ledger, open_positions,pnldf,last_processed = fs.initiate_variables(0,aux_name,aux_files)
for timestamp in symbhistorical['date'].to_list():
    if timestamp < sim_start_date:
        continue
    try:
        print(timestamp)
        # input('Enter to Continue')
        expiry = fs.find_expiry(timestamp,expiry_dir)
        spot = float(symbhistorical['close'][symbhistorical['date']==timestamp])
        vixnow = float(vixhistorical['close'][vixhistorical['date'] == timestamp])
        s.range('j9').value = spot
        if type(expiry) == type(timestamp):
            days_to_expiry = (expiry - timestamp).days
            contractsdir = expiry_dir+expiry.strftime(final_dayformat)+'/'
            contracts = {}
            for item in os.listdir(contractsdir):
                if contractsdir+item not in database.keys():
                    database[contractsdir + item] = pd.read_csv(contractsdir+item)
                contracts[item.replace('.csv','')] = database[contractsdir + item]


            opt_ltps,opt_ois = fs.update_ltp_oi_ohlc(timestamp,contracts)

            if not len(opt_ltps):
                continue
            l, u = spot * (1 - prob_range), spot * (1 + prob_range)
            mean,sd = get_stats_from_vix(timestamp,vix_range_percent,vixnow,expiry,
                                         vixhistorical[vixhistorical['date'] < sim_start_date],symbhistorical[symbhistorical['date'] < sim_start_date])
            ps,cs = fs.spread_evs_custom_new_model(opt_ltps,opt_ois,l,u,mean,sd)
            l,u = spot*(1-trade_range),spot*(1+trade_range)
            ps,cs = fs.filter_for_tradable_spreads(open_positions,ps,cs,l,u)
            xl_put_spreads = ps
            xl_call_spreads = cs
            ps,cs = fs.add_debit_spreads(ps,cs)
            tradelist = fs.find_trades(open_positions,ps,cs,percentile)
            if days_to_expiry == 0 and len(open_positions):
                ledger, open_positions = show_tradelist_sq(ledger, open_positions, tradelist, timestamp, opt_ltps,
                                                           xl_put_spreads, xl_call_spreads, expiry, find_lotsize(symb),
                                                           spot)
                pickle.dump(open_positions, open(aux_files + 'open_positions_' + aux_name, 'wb'))
                pickle.dump(ledger, open(aux_files + 'ledger_' + aux_name, 'wb'))
            else:
                ledger, open_positions = show_tradelist_ohlc_backtest(ledger, open_positions, tradelist, timestamp,
                                                                      opt_ltps,
                                                                      xl_put_spreads, xl_call_spreads, expiry,
                                                                      find_lotsize(symb))
                try:
                    fs.payoff_graph(open_positions, opt_ltps, s)
                except:
                    pass
                pickle.dump(open_positions, open(aux_files + 'open_positions_' + aux_name, 'wb'))
                pickle.dump(ledger, open(aux_files + 'ledger_' + aux_name, 'wb'))



    except Exception as e:
        print(e)

