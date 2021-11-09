#From Version _ of live option adjust to changing the execution part of the system.
#   1. Tradelist dict will be sent to the order placing function instead of one trade at a time
#   2. This function will now place a set of orders (either calls or puts) first to check if the options are tradable and margin is available
#   3. Once it succeeds, it will place limit order for the bigger priced contract first and wait for 30 seconds to get a fill
#       a. If it gets the fill during this time, it will change the smaller price limit order to market.
#   4. If Step 3 succeeds completely, it will repeat it for the remaining pair of trades (if any)
#   5. It will return the executed tradelist to modify the open positions

import pandas as pd
import time
import datetime
import zd
import numpy as np
import os
import pickle
import xlwings as xw
from nsetools import Nse
import math

def round_down(x):
    a = 0.05
    return math.floor(x / a) * a
nse=Nse()
index_codes = nse.get_index_list()

file_path='xlwings.xlsx'
s=xw.Book(file_path).sheets['Sheet3']
allins = zd.kite.instruments()
market_open = datetime.time(9,15,00)
market_close = datetime.time(15,30,00)
curr_ins = {}
iupac="%m%d%Y-%H%M"
for ins in allins:
    curr_ins[ins['instrument_token']] = ins
types=[]
segments=[]
exchanges=[]
ticksizes=[]
indices=[]
last_tick={}
ltp_by_symbol={}

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
    if index['name']=='INDIA VIX':
        # print(index)
        vix_instrument=index
        break
for index in indices:
    if index['name']=='NIFTY 50':
        # print(index)
        nifty_50_instrument=index
        break

def get_ltps():
    fname = os.listdir('optionticks')
    fname = fname[len(fname)-1]
    return pickle.load(open('optionticks/'+fname,"rb"))

def get_historical(instrument_token,fdate,tdate,interv):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    if filename in os.listdir('get_historical'):
        df = pd.read_csv('get_historical/' + filename)
        df['date'] = df[['date']].apply(pd.to_datetime)
        return df
    if interv == "day" and (tdate-fdate).days > 1500:
        fdates=[fdate]
        newtdate=fdate+day1500
        tdates=[newtdate]

        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day1500)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(zd.kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day70 = datetime.timedelta(days=70)
    day1 = datetime.timedelta(days=1)
    if interv == '5minute':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(zd.kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
fdate=datetime.datetime(2011,1,1)
tdate=datetime.datetime(2020,12,31)
dateformat='%Y-%m-%d'

vix=get_historical(vix_instrument['instrument_token'],fdate,tdate, "5minute")
nifty=get_historical(nifty_50_instrument['instrument_token'],fdate,tdate, "5minute")
vix=vix.loc[vix['date'].isin(nifty['date'])]
nifty=nifty.loc[nifty['date'].isin(vix['date'])]

timeframe=5 #in minutes
def tradables(instru, lstrike, ustrike):
    global timeframe
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100
    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days

    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            daystoexpiry = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT':
                if daystoexpiry == expfut:
                    filtered.append(ins)
            elif daystoexpiry == expopt and ins['strike'] >= lstrike and ins['strike'] <= ustrike:
                filtered.append(ins)

    now=datetime.datetime.now()
    trading_sessions=int(((expopt*375)/timeframe)+min(75,int(12*(datetime.datetime.combine(now.date(),market_close)-now).seconds/3600)))
    return filtered, trading_sessions

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

# all_tradables, trading_sessions = tradables(allins,15000,16000)
# nifty_ltp = get_ltps()[nifty_50_instrument['instrument_token']]
# nifty_vix=pd.DataFrame()
# nifty_vix['date'] = vix['date']
# nifty_vix['vix_close'] = vix['close']
# nifty_vix['nifty_close'] = nifty['close']
# nifty_vix['ret'] = nifty['close'].shift(-trading_sessions)
# nifty_vix = nifty_vix[nifty_vix['ret'].notna()]
# nifty_vix['dist'] = nifty_ltp*nifty_vix['ret']/nifty_vix['nifty_close']
# mindist=np.percentile(nifty_vix['dist'],1)
# maxdist=np.percentile(nifty_vix['dist'],99)
# nifty_vix = nifty_vix[nifty_vix.dist > mindist]
# nifty_vix = nifty_vix[nifty_vix.dist < maxdist]
# nifty_vix['dist'].plot.hist(bins=100,alpha=0.5)
option_ltps = {}
options_historical_folder = 'current_options/'
def optionltp(option_prices):
    global option_ltps
    for item in option_prices:
        option_ltps[str(item['strike'])+item['type']] = item['last_price']

def get_option_prices(timestamp):
    fname = timestamp.strftime(iupac)
    if fname in os.listdir('temp'):
        return pickle.load(open('temp/'+fname, 'rb'))
    expiries = [datetime.datetime.strptime(item,dateformat) for item in os.listdir(options_historical_folder)]
    expiries.sort()
    for expiry in expiries:
        if timestamp<=expiry:
            # print('Expiry:', expiry)
            break
    foldername = options_historical_folder+expiry.strftime(dateformat)+'/rec/'
    files = os.listdir(foldername)
    optionslist=[]
    for file in files:
        df = pd.read_csv(foldername + file)
        df['date'] = df[['date']].apply(pd.to_datetime)
        if timestamp in df['date'].to_list():
            index = df['date'].to_list().index(timestamp)
            ltp = df['close'][index]
            if 'PE' in file:
                strike = int(file.replace('PE.csv',''))
                type = 'PE'
            else:
                strike = int(file.replace('CE.csv', ''))
                type = 'CE'
            optionslist.append({'strike':strike,'last_price':ltp,'type':type})
    pickle.dump(optionslist,open('temp/'+fname, "wb"))
    return optionslist

def find_expiry():
    global trading_sessions
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100

    for ins in allins:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days
                exp_opt_date = ins['expiry']
    return exp_opt_date
def get_ev(option_prices,spot,vix_now,trading_sessions,b):
    ndis = nifty_distribution((1-b)*vix_now,(1+b)*vix_now,trading_sessions)
    ndis = [round((1 + item/100) * spot) for item in ndis]
    possible_spots = np.arange(int(min(ndis)),int(max(ndis)),1)
    df=pd.DataFrame()
    df['spot'] = possible_spots
    freq=[]
    for item in df['spot']:
        freq.append(ndis.count(item))
    df['freq'] = freq
    cev={}
    pev={}
    ppop={}
    cpop={}
    for d in option_prices:
        if d['type']=='CE':
            label=str(d['strike'])+d['type']
            df[label] = df['spot'] - d['strike']
            df[label].values[df['spot'] < d['strike']] = 0
            df[label] = df[label] - d['last_price']
            cev[label] = (df[label]*df['freq']).mean()
            cpop = ((df[label]>0)*df['freq']).sum()/df['freq'].sum()
        elif d['type']=='PE':
            label=str(d['strike'])+d['type']
            df[label] = d['strike'] - df['spot']
            df[label].values[df['spot'] > d['strike']] = 0
            df[label] = df[label] - d['last_price']
            pev[label] = (df[label]*df['freq']).mean()
            ppop = ((df[label]>0)*df['freq']).sum()/df['freq'].sum()
        else:
            print('Option Type Not found',d)
    return cev, pev, cpop, ppop


try:
    ledger = pickle.load(open('current_ledger','rb'))
except:
    ledger = pd.DataFrame()
def manage_positions(cev,pev,percentile):
    global open_positions
    tradelist={}
    if len(open_positions) == 0:
        for key in cev.keys():
            if cev[key] == max(cev.values()):
                tradelist[key] = 1
            if cev[key] == min(cev.values()):
                tradelist[key] = -1
        for key in pev.keys():
            if pev[key] == max(pev.values()):
                tradelist[key] = 1
            if pev[key] == min(pev.values()):
                tradelist[key] = -1
        return tradelist
    else:
        call_buy_ev = np.percentile(pd.Series(cev.values()),100 - percentile)
        put_buy_ev = np.percentile(pd.Series(pev.values()), 100 - percentile)
        call_sell_ev = np.percentile(pd.Series(cev.values()), percentile)
        put_sell_ev = np.percentile(pd.Series(pev.values()), percentile)
        tradelist={}
        opkeys=list(open_positions.keys())
        for key in opkeys:
            if key in cev.keys() or key in pev.keys():
                type = key[-2:]
                if type=='CE':
                    if open_positions[key] == 1:
                        if cev[key] < call_buy_ev:
                            if key in tradelist.keys():
                                tradelist[key] -= 1
                            else:
                                tradelist[key] = -1
                            for key2 in cev.keys():
                                if cev[key2] == max(cev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] += 1
                                    else:
                                        tradelist[key2] = 1
                                    continue
                    else:
                        if cev[key] > call_sell_ev:
                            if key in tradelist.keys():
                                tradelist[key] += 1
                            else:
                                tradelist[key] = 1
                            for key2 in cev.keys():
                                if cev[key2] == min(cev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] -= 1
                                    else:
                                        tradelist[key2] = -1
                                    continue
                else:
                    if open_positions[key] == 1:
                        if pev[key] < put_buy_ev:
                            if key in tradelist.keys():
                                tradelist[key] -= 1
                            else:
                                tradelist[key] = -1
                            for key2 in pev.keys():
                                if pev[key2] == max(pev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] += 1
                                    else:
                                        tradelist[key2] = 1
                                    continue
                    else:
                        if pev[key] > put_sell_ev:
                            if key in tradelist.keys():
                                tradelist[key] += 1
                            else:
                                tradelist[key] = 1
                            for key2 in pev.keys():
                                if pev[key2] == min(pev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] -= 1
                                    else:
                                        tradelist[key2] = -1
                                    continue
        poplist = []
        if len(tradelist)>0:
            print('Open Positions: ',open_positions)
            print('Tradelist: ',tradelist)
        return tradelist
    return tradelist

def send_option_market_order(type, token, qty):
    if type == 'buy':
        try:
            order_id = zd.kite.place_order(tradingsymbol=token,
                                           exchange=zd.kite.EXCHANGE_NFO,
                                           transaction_type=zd.kite.TRANSACTION_TYPE_BUY,
                                           quantity=qty,
                                           order_type=zd.kite.ORDER_TYPE_MARKET,
                                           product=zd.kite.PRODUCT_NRML,
                                           variety='regular')

            print("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            print("Order placement failed: {}".format(e))
    else:
        try:
            order_id = zd.kite.place_order(tradingsymbol=token,
                                           exchange=zd.kite.EXCHANGE_NFO,
                                           transaction_type=zd.kite.TRANSACTION_TYPE_SELL,
                                           quantity=qty,
                                           order_type=zd.kite.ORDER_TYPE_MARKET,
                                           product=zd.kite.PRODUCT_NRML,
                                           variety='regular')

            print("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            print("Order placement failed: {}".format(e))

def send_option_limit_order(type, token, qty,price):
    if type == 'buy':
        try:
            order_id = zd.kite.place_order(tradingsymbol=token,
                                           exchange=zd.kite.EXCHANGE_NFO,
                                           transaction_type=zd.kite.TRANSACTION_TYPE_BUY,
                                           quantity=qty,
                                           order_type=zd.kite.ORDER_TYPE_LIMIT,
                                           price=price,
                                           product=zd.kite.PRODUCT_NRML,
                                           variety='regular')

            print("Order placed. ID is: {}".format(order_id))
            return order_id
        except Exception as e:
            print("Order placement failed: {}".format(e))
            return e
    else:
        try:
            order_id = zd.kite.place_order(tradingsymbol=token,
                                           exchange=zd.kite.EXCHANGE_NFO,
                                           transaction_type=zd.kite.TRANSACTION_TYPE_SELL,
                                           quantity=qty,
                                           order_type=zd.kite.ORDER_TYPE_LIMIT,
                                           price = price,
                                           product=zd.kite.PRODUCT_NRML,
                                           variety='regular')

            print("Order placed. ID is: {}".format(order_id))
            return order_id
        except Exception as e:
            print("Order placement failed: {}".format(e))
            return e


def trade_tradelist(tradelist,scalp,wait_time):
    if len(open_positions) == 0:
        for key in tradelist.keys():
            if tradelist[key] > 0:
                otype = 'buy'
            else:
                otype = 'sell'
            token = curr_ins[label_to_token[key]]['tradingsymbol']
            send_option_market_order(otype, token, abs(tradelist[key] * 75))
    calls = {}
    puts = {}
    print(tradelist)
    finaltradelist = {}
    for key in tradelist:
        if 'CE' in key:
            calls[key] = option_ltps[key]
        else:
            puts[key] = option_ltps[key]
    oids = {}
    for key in calls.keys():
        qty = abs(75*tradelist[key])
        if tradelist[key]>0:
            otype = 'buy'
            price = 0.05
        else:
            otype = 'sell'
            price = 2*calls[key]
        token = curr_ins[label_to_token[key]]['tradingsymbol']
        oid = send_option_limit_order(otype, token, qty, price)
        oids[key] = oid
    time.sleep(0.2)
    status = 1
    for key in oids.keys():
        oid = oids[key]
        oh = zd.kite.order_history(oid)
        if oh[-1]['status'] != 'OPEN':
            status = 0
    if not status:
        print('Calls Order Placement Failed')
        for key in oids.keys():
            oid = oids[key]
            try:
                zd.kite.cancel_order(order_id=oid,variety='regular')
            except:
                pass
    else:
        if len(calls) == 2:
            calls = dict(sorted(calls.items(), key=lambda item: item[1],reverse=True))
            key = list(calls.keys())[0]
            if tradelist[key]>0:
                zd.kite.modify_order(price=round_down((1-scalp)*calls[key]),
                                 order_id=oids[key],
                                 variety='regular')
            else:
                zd.kite.modify_order(price=round_down((1+scalp) * calls[key]),
                                     order_id=oids[key],
                                     variety='regular')
            t1 = time.time()
            calls_traded = 0
            print('Waiting for Fill, ', key)
            while time.time()-t1<wait_time:
                oid = oids[key]
                oh = zd.kite.order_history(oid)
                if oh[-1]['status'] == 'COMPLETE':
                    key2 = list(calls.keys())[1]
                    qty = abs(75 * tradelist[key2])
                    if tradelist[key2] > 0:
                        otype = 'buy'
                    else:
                        otype = 'sell'
                    token = curr_ins[label_to_token[key2]]['tradingsymbol']
                    zd.kite.modify_order(price=round_down(calls[key2]*1.5),
                                         order_id=oids[key2],
                                         variety='regular')
                    calls_traded = 1
            if calls_traded:
                print('Calls Traded')
                for key in calls.keys():
                    finaltradelist[key] = tradelist [key]
            else:
                print('Time Over for Calls')
                for key in calls.keys():
                    oid = oids[key]
                    try:
                        zd.kite.cancel_order(order_id=oid, variety='regular')
                    except:
                        pass
    oids = {}
    for key in puts.keys():
        qty = abs(75 * tradelist[key])
        if tradelist[key] > 0:
            otype = 'buy'
            price = 0.05
        else:
            otype = 'sell'
            price = 2 * puts[key]
        token = curr_ins[label_to_token[key]]['tradingsymbol']
        oid = send_option_limit_order(otype, token, qty, price)
        oids[key] = oid
    time.sleep(0.2)
    status = 1
    for key in oids.keys():
        oid = oids[key]
        oh = zd.kite.order_history(oid)
        if oh[-1]['status'] != 'OPEN':
            status = 0
    if not status:
        print('puts Order Placement Failed')
        for key in oids.keys():
            oid = oids[key]
            try:
                zd.kite.cancel_order(order_id=oid, variety='regular')
            except:
                pass
    else:
        if len(puts) == 2:
            puts = dict(sorted(puts.items(), key=lambda item: item[1], reverse=True))
            key = list(puts.keys())[0]
            if tradelist[key] > 0:
                zd.kite.modify_order(price=round_down((1 - scalp) * puts[key]),
                                     order_id=oids[key],
                                     variety='regular')
            else:
                zd.kite.modify_order(price=round_down((1 + scalp) * puts[key]),
                                     order_id=oids[key],
                                     variety='regular')
            t1 = time.time()
            puts_traded = 0
            print('Waiting for Fill, ', key)
            while time.time() - t1 < wait_time:
                oid = oids[key]
                oh = zd.kite.order_history(oid)
                if oh[-1]['status'] == 'COMPLETE':
                    key2 = list(puts.keys())[1]
                    qty = abs(75 * tradelist[key2])
                    if tradelist[key2] > 0:
                        otype = 'buy'
                    else:
                        otype = 'sell'
                    token = curr_ins[label_to_token[key2]]['tradingsymbol']
                    zd.kite.modify_order(price=round_down(puts[key2] * 1.5),
                                         order_id=oids[key2],
                                         variety='regular')
                    puts_traded = 1
            if puts_traded:
                print('puts Traded')
                for key in puts.keys():
                    finaltradelist[key] = tradelist[key]
            else:
                print('Time Over for puts')
                for key in puts.keys():
                    oid = oids[key]
                    try:
                        zd.kite.cancel_order(order_id=oid, variety='regular')
                    except:
                        pass
    return finaltradelist

def show_tradelist(tradelist,option_prices):
    scalp = 0.005
    wait_time = 30
    global ledger, open_positions
    tradelist = dict(sorted(tradelist.items(), key=lambda item: item[1],reverse=True))
    if len(tradelist)==0:
        return
    tradelist = trade_tradelist(tradelist,scalp,wait_time)
    if len(tradelist)==0:
        return

    plist = []
    for key in tradelist.keys():
        if key in open_positions.keys():
            open_positions[key] += tradelist[key]
            if open_positions[key] == 0:
                plist.append(key)
        else:
            open_positions[key] = tradelist[key]
    for key in plist:
        open_positions.pop(key)
    for key in tradelist.keys():
        nr = {'timestamp': datetime.datetime.now(), 'expiry': find_expiry(), 'instrument': key,
              'qty': tradelist[key] * 75, 'price': option_ltps[key]}
        ledger = ledger.append(nr, ignore_index=True)
    print(tradelist)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    sqpnl = 0
    for key in open_positions.keys():
        if open_positions[key] == 1:
            sqpnl += option_ltps[key]*75
        else:
            sqpnl -= option_ltps[key]*75
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
    # for key in tradelist.keys():
    #     if tradelist[key]>0:
    #         otype = 'buy'
    #     else:
    #         otype = 'sell'
    #     token = curr_ins[label_to_token[key]]['tradingsymbol']
    #     send_option_order(otype, token, abs(tradelist[key]*75))
    openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
    s.range('F10').value = booked_pnl
    s.range('H9').value = sum(booked_pnl.values())
    s.range('H2').value = sqpnl
    s.range('H5').value = openpnl
    s.range('Z1').value = print_ledger['value'].sum() + sqpnl
    pickle.dump(open_positions, open('open_option_positions', 'wb'))
    pickle.dump(ledger, open('current_ledger', 'wb'))
    # if len(tradelist)!=0:
    #     payoff_diagram(option_prices)
        # input("Enter to Continue")
def squareoff_all(option_prices):
    global open_positions
    tradelist = {}
    poplist = []
    for key in open_positions.keys():
        for item in option_prices:
            if str(item['strike'])+item['type'] == key:
                tradelist[key] = -open_positions[key]
    return tradelist

def filteroptions(option_prices,spot,strikepercent):
    filopts = []
    minstrike = spot*(1-strikepercent)
    maxstrike = spot * (1+strikepercent)
    for item in option_prices:
        if minstrike<item['strike']<maxstrike:
            filopts.append(item)

    return filopts

sim_start_date = datetime.datetime(2021,6,1)
sim_stop_date = datetime.datetime(2021,6,22)
nifty2021 = get_historical(nifty_50_instrument['instrument_token'],sim_start_date,sim_stop_date,'5minute')
vix2021 = get_historical(vix_instrument['instrument_token'],sim_start_date,sim_stop_date,'5minute')
expiries = [datetime.datetime.strptime(item,dateformat) for item in os.listdir(options_historical_folder)]
# for item in nifty2021['date']:
#     print(len(get_option_prices(item)))
def payoff_diagram(option_prices):
    ndis = nifty_distribution(0.9 * vix_now, 1.1 * vix_now, trading_sessions)
    ndis = [round((1 + item / 100) * spot) for item in ndis]
    possible_spots = np.arange(int(min(ndis)), int(max(ndis)), 1)
    df = pd.DataFrame()
    df['spot'] = possible_spots
    freq = []
    for item in df['spot']:
        freq.append(ndis.count(item))
    df['freq'] = freq
    for d in option_prices:
        if d['type'] == 'CE':
            label = str(d['strike']) + d['type']
            df[label] = df['spot'] - d['strike']
            df[label].values[df['spot'] < d['strike']] = 0
            df[label] = df[label] - d['last_price']
        elif d['type'] == 'PE':
            label = str(d['strike']) + d['type']
            df[label] = d['strike'] - df['spot']
            df[label].values[df['spot'] > d['strike']] = 0
            df[label] = df[label] - d['last_price']
        else:
            print('Option Type Not found', d)
    df['payoff'] = 0
    for key in open_positions.keys():
        if key not in df.columns:
            return
        df['payoff'] = df['payoff']+open_positions[key]*75*df[key]
    pdf = df[['spot','payoff']]
    s.range('AB1').value = pdf[pdf['spot']%10==0].set_index('spot',drop=True)

def get_live_option_prices():
    last_file = os.listdir('optionticks')
    last_file = last_file[len(last_file)-1]
    last_tick = pickle.load(open('optionticks/'+last_file,'rb'))
    optionlist = []
    for key in last_tick.keys():
        optionlist.append({'strike':curr_ins[key]['strike'],'last_price':last_tick[key]['last_price'],'type':curr_ins[key]['instrument_type']})
        label = str(curr_ins[key]['strike'])+curr_ins[key]['instrument_type']
        if label not in label_to_token.keys():
            label_to_token[label] = key

    return optionlist

def next_weekly_expiry(instru):
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100

    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days
                exp_opt_date = ins['expiry']
    expopt = np.busday_count(datetime.datetime.now().date(), exp_opt_date) + 1
    return expopt


# timestamp = nifty2021['date'][6]
label_to_token={}
try:
    open_positions = pickle.load(open('open_option_positions','rb'))
except:
    open_positions = {}
while(1==1):

    sim_start_date = datetime.datetime.combine(datetime.datetime.now().date(),market_open)
    sim_stop_date = datetime.datetime.now()
    nifty2021 = get_historical(nifty_50_instrument['instrument_token'], sim_start_date, sim_stop_date, '5minute')
    if sum(open_positions.values())!=0:
        break

    till = len(nifty2021)-1
    start = max(0, till - 30)
    ohlc = nifty2021.loc[start:till].set_index('date',drop=True)
    s.range('J1').value = ohlc
    percentile = 50
    strike_range_percent = 0.025
    b = 0.15
    option_prices = get_live_option_prices()
    optionltp(option_prices)
    if len(option_prices) == 0:
        continue

    q = nse.get_index_quote('nifty 50')
    spot = q['lastPrice']
    q = nse.get_index_quote('india vix')
    vix_now = q['lastPrice']
    expopt = next_weekly_expiry(allins)
    trading_sessions = expopt*75 - int((datetime.datetime.now() - datetime.datetime.combine(datetime.datetime.now().date(),datetime.time(9,15,00))).seconds/300)
    tradable_option_prices = filteroptions(option_prices, spot,strike_range_percent)
    if trading_sessions<10:
        if len(open_positions) > 0:
            tradelist = squareoff_all(option_prices)
            if sum(open_positions.values()) != 0:
                break
            show_tradelist(tradelist, option_prices)
        else:
            continue

    cev, pev, cpop, ppop=get_ev(tradable_option_prices,spot,vix_now,trading_sessions,b)
    s.range('A1').value = cev
    s.range('C1').value = pev

    tradelist = manage_positions(cev,pev,percentile)
    if sum(open_positions.values())!=0:
        break
    show_tradelist(tradelist,option_prices)
    time.sleep(5)

