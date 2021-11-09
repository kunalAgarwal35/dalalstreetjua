import zd
import pickle
import os
import pandas as pd
import numpy as np
import datetime
import time
import xlwings as xw
file_path='xlwings.xlsx'
s=xw.Book(file_path).sheets['Sheet4']
option_ticks_dir = 'optionticks/'
option_ltps = {}
print_option_ltps = {}
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


nifty50list = pd.read_csv('ind_nifty50list.csv')['Symbol'].to_list()
expiry = datetime.datetime(2021,6,24).date()
option_tokens = {}
for token in curr_ins.keys():
    if curr_ins[token]['instrument_type'] in ['CE','PE','FUT'] and curr_ins[token]['exchange'] == 'NFO':
        if curr_ins[token]['expiry'] ==  expiry:
            symbol = curr_ins[token]['name']
            if symbol in nifty50list:
                if symbol not in option_tokens.keys():
                    option_tokens[symbol] = {}
                option_tokens[symbol][token] = curr_ins[token]

fdate = datetime.datetime(2021,6,1,9,15,0)
tdate = datetime.datetime.now()
interval = '5minute'
oi = 1
#
# for symbol in option_tokens.keys():
#     for ins in option_tokens[symbol].keys():
#         print(option_tokens[symbol][ins]['tradingsymbol'])
#         try:
#             zd.get_historical(option_tokens[symbol][ins]['instrument_token'],fdate,tdate,interval,oi)
#         except Exception as e:
#             print(e)

for symbol in list(option_tokens.keys())[5:]:
    print(symbol)
    fut = 0
    ohlcs = {}
    for key in option_tokens[symbol].keys():
        if option_tokens[symbol][key]['instrument_type'] == 'FUT':
            fut = key
        try:
            ohlcs[key] = zd.get_historical(option_tokens[symbol][key]['instrument_token'],fdate,tdate,interval,oi)
        except:
            pass

    for i in range(0,len(ohlcs[fut])-1):
        timestamp = ohlcs[fut]['date'][i]
        fut_price = ohlcs[fut]['close'][i]
        net_oi = 0
        product_oi_price = 0
        for key in ohlcs.keys():
            if key != fut:
                if timestamp in ohlcs[key]['date'].to_list():
                    oi = int(ohlcs[key].loc[ohlcs[key]['date']==timestamp]['oi'])
                    net_oi += oi
                    ins = curr_ins[key]
                    if ins['instrument_type'] == 'CE':
                        price = ins['strike'] + float(ohlcs[key].loc[ohlcs[key]['date']==timestamp]['close'])
                    elif ins['instrument_type'] == 'PE':
                        price = ins['strike'] - float(ohlcs[key].loc[ohlcs[key]['date'] == timestamp]['close'])
                    product_oi_price += price*oi
        projected_price = product_oi_price/net_oi
        out = 100*np.log(projected_price/fut_price)
        print(out)
        if abs(out)>4:
            s.range('K1').value = out
            s.range('A1').value = ohlcs[fut][i:i+75].set_index('date',drop=True)





