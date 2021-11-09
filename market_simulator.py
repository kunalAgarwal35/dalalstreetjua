import pickle
import os
import pandas as pd
import numpy as np
import xlwings as xw
import time
import datetime

tickdir='dayticks/'
iupac="%m%d%Y-%H%M%S-%f"
dayformat="%y-%m-%d"
output='market_simulate/market_wings.xlsx'
sheet1=xw.Book(output).sheets['Sheet1']
ticklist = os.listdir(tickdir)
dates_list = [datetime.datetime.strptime(date, dayformat).date() for date in ticklist]
df=pd.DataFrame(columns=['timestamp','volume','open','high','low','close','oi','buys','sells'])

#Initiate and test
sheet1.range('A1').value=df.set_index('timestamp',drop=True)

#Gather inputs
tickdict={}
ts={}
st={}
def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename, 'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
        st[instrument['tradingsymbol']] = instrument['instrument_token']
    return d

apr = get_lot_size_dict('April_kite.instruments')
may = get_lot_size_dict('May.instruments')
jun = get_lot_size_dict('june.instruments')

def welcome():
    global tickdict
    try:
        date=sheet1.range('K2').value.date()
        if date not in dates_list:
            sheet1.range('L2').value = 'Date not Found'
        else:
            sheet1.range('L2').value = 'Date Found'
    except:
        sheet1.range('L2').value = 'Invalid date'

    if sheet1.range('K3').value:
        t1=time.time()
        print('Loading Ticks')
        sheet1.range('L3').value = 'Loading Ticks...'
        tickdict = pickle.load(open(date.strftime(tickdir+dayformat), 'rb'))
        print(str(len(tickdict))+' Ticks loaded in '+str(time.time()-t1)+' seconds')
        sheet1.range('L3').value = str(len(tickdict))+' Ticks loaded in '+str(int(time.time()-t1))+' seconds'

def filter_tickdict():
    global tickdict
    tempdict={}
    tradingsymbol = sheet1.range('K4').value.upper()
    for timestamp in tickdict.keys():
        if st[tradingsymbol] in tickdict[timestamp].keys():
            tempdict[timestamp]=tickdict[timestamp][st[tradingsymbol]]
    tickdict=tempdict



welcome()
filter_tickdict()
last_vol=0
def timestamp_label(input):
    return input.strftime('%H:%M')
def update_depth(depth):
    buy=depth['buy']
    sell=depth['sell']
    dfbuy=pd.DataFrame(columns=['orders','quantity','price'])
    dfsell = pd.DataFrame(columns=['price', 'quantity', 'orders'])
    for item in buy:
        dfbuy = dfbuy.append(item,ignore_index=True)
    for item in sell:
        dfsell = dfsell.append(item,ignore_index=True)
    sheet1.range('O3').value = dfbuy.set_index('price', drop=True)
    sheet1.range('R3').value = dfsell.set_index('price', drop=True)
def process_tick(key):
    global df,last_vol
    nc = sheet1.range('K5').value
    if len(df)==0:
        nr={'timestamp':timestamp_label(datetime.time(key.hour, key.minute))}
        nr['volume'] = 0
        nr['open'] = tickdict[key]['last_price']
        nr['high'] = tickdict[key]['last_price']
        nr['low'] = tickdict[key]['last_price']
        nr['close'] = tickdict[key]['last_price']
        nr['oi'] = tickdict[key]['oi']
        nr['buys'] = tickdict[key]['buy_quantity']
        nr['sells'] = tickdict[key]['sell_quantity']
        last_vol = tickdict[key]['volume']
        while len(df)<nc:
            df = df.append(nr,ignore_index=True)
        sheet1.range('A1').value = df.set_index('timestamp', drop=True)
        update_depth(tickdict[key]['depth'])
    else:
        nr = {'timestamp': timestamp_label(datetime.time(key.hour, key.minute))}
        if nr['timestamp'] == df['timestamp'].iloc[-1]:
            #Update the same row procedure
            ltp = tickdict[key]['last_price']
            volume = df['volume'].iloc[-1]+tickdict[key]['volume']-last_vol
            last_vol = tickdict[key]['volume']
            if ltp > df['high'].iloc[-1]:
                df.at[df.index[-1],'high'] = ltp
            if ltp < df['low'].iloc[-1]:
                df.at[df.index[-1],'low'] = ltp
            df.at[df.index[-1],'close'] = ltp
            df.at[df.index[-1], 'volume'] = volume
            df.at[df.index[-1], 'oi'] = tickdict[key]['oi']
            df.at[df.index[-1], 'buys'] = tickdict[key]['buy_quantity']
            df.at[df.index[-1], 'sells'] = tickdict[key]['sell_quantity']
            sheet1.range('A1').value = df.set_index('timestamp', drop=True)
            update_depth(tickdict[key]['depth'])
        else:
            nr = {'timestamp': timestamp_label(datetime.time(key.hour, key.minute))}
            nr['volume'] = 0
            nr['open'] = tickdict[key]['last_price']
            nr['high'] = tickdict[key]['last_price']
            nr['low'] = tickdict[key]['last_price']
            nr['close'] = tickdict[key]['last_price']
            nr['oi'] = tickdict[key]['oi']
            nr['buys'] = tickdict[key]['buy_quantity']
            nr['sells'] = tickdict[key]['sell_quantity']
            last_vol = tickdict[key]['volume']
            df = df.append(nr, ignore_index=True)
            if len(df) > nc:
                df = df.iloc[1:, :]
            sheet1.range('A1').value = df.set_index('timestamp', drop=True)
            update_depth(tickdict[key]['depth'])





df=pd.DataFrame(columns=['timestamp','volume','open','high','low','close','oi','buys','sells'])
for key in list(tickdict.keys()):
    process_tick(key)

