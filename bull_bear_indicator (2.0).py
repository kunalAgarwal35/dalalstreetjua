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
expiry = datetime.datetime(2021,7,29).date()
option_tokens = {}
for token in curr_ins.keys():
    if curr_ins[token]['instrument_type'] in ['CE','PE','FUT'] and curr_ins[token]['exchange'] == 'NFO':
        if curr_ins[token]['expiry'] ==  expiry:
            symbol = curr_ins[token]['name']
            if symbol in nifty50list:
                if symbol not in option_tokens.keys():
                    option_tokens[symbol] = {}
                option_tokens[symbol][token] = curr_ins[token]



#
# for symbol in option_tokens.keys():
#     for ins in option_tokens[symbol].keys():
#         print(option_tokens[symbol][ins]['tradingsymbol'])
#         try:
#             zd.get_historical(option_tokens[symbol][ins]['instrument_token'],fdate,tdate,interval,oi)
#         except Exception as e:
#             print(e)
def send_to_ledger(entry,exit,entry_time,exit_time,symbol):
    if 'ledger.csv' in os.listdir('ledgers'):
        ledger = pd.read_csv('ledgers/ledger.csv')
    else:
        ledger = pd.DataFrame()
    nr = {'entry':entry,'exit':exit,'entry_time':entry_time,'exit_time':exit_time,'symbol':symbol,'gain':100*np.log(exit/entry)}
    ledger = ledger.append(nr,ignore_index=True)
    ledger.to_csv('ledgers/ledger.csv',index = False)
    s.range('A1').value = ledger
    s.range('K1').value = ledger['gain'].mean()
def get_historical(instrument_token,fdate,tdate,interv,oi):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    # filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    # if filename in os.listdir('get_historical'):
    #     df = pd.read_csv('get_historical/' + filename)
    #     df['date'] = df[['date']].apply(pd.to_datetime)
    #     return df
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
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        # df.to_csv('get_historical/'+filename,index=False)
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
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        # df.to_csv('get_historical/'+filename,index=False)
        return df
intrade = {}
out_df = {}
max = {}
for symbol in option_tokens.keys():
    intrade[symbol] = 0
    max[symbol] = 0
    out_df[symbol] = 0

while (1==1):
    print('initializing...')
    for symbol in list(option_tokens.keys()):
        try:
            tdate = datetime.datetime.now()
            min30 = datetime.timedelta(minutes=10)
            fdate = tdate - min30
            interval = '5minute'
            oi = 1
            # print(symbol)
            fut = 0
            ohlcs = {}
            for key in option_tokens[symbol].keys():
                if option_tokens[symbol][key]['instrument_type'] == 'FUT':
                    fut = key
                try:
                    ohlcs[key] = get_historical(option_tokens[symbol][key]['instrument_token'],fdate,tdate,interval,oi)[-1:].reset_index(drop=True)
                except:
                    # print(symbol, ' get historical failed')
                    pass
            print('Checking in : ',len(option_tokens[symbol].keys()), ' contracts for ',symbol)

            i = 0
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
            out_df[symbol] = out
            s.range('A1').value = out_df
            # print(out)
            if out>4.3:
                if not intrade[symbol]:
                    entry = fut_price
                    entry_time = timestamp
                    print(symbol,'---BUY--->',str(entry), '(',entry_time,')')
                    intrade[symbol] = 1
                else:
                    if out>max[symbol]:
                        max[symbol] = out
            elif out<0.70*max[symbol] or out<3.9:
                if intrade[symbol]:
                    exit = fut_price
                    exit_time = timestamp
                    print(symbol, '---SELL--->', str(exit), '(', exit_time, ')')
                    # send_to_ledger(entry,exit,entry_time,exit_time,symbol)
                    intrade[symbol] = 0
                    max[symbol] = 0
        except Exception as e:
            print(e)





