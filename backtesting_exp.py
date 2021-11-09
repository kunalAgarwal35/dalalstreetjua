import pandas as pd
import pickle, os
import numpy as np
import datetime
from multiprocessing import Process, Manager
import _thread as thread
import time
tickdir='mayticks/'
iupac="%m%d%Y-%H%M%S-%f"
ticklist = os.listdir(tickdir)
dates_list = [datetime.datetime.strptime(date, iupac) for date in ticklist]
ts={}
def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename, 'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
    return d
lot_size_dict = get_lot_size_dict('April_kite.instruments')
may_lot_size_dict = get_lot_size_dict('May.instruments')
june_lot_size_dict = get_lot_size_dict('june.instruments')
for token in may_lot_size_dict.keys():
    lot_size_dict[token] = may_lot_size_dict[token]
for token in june_lot_size_dict.keys():
    lot_size_dict[token] = june_lot_size_dict[token]
st={}
for key in ts.keys():
    st[ts[key]]=key
def updatedict(argu):
    d=argu[0]
    listofdates=argu[1]
    for item in listofdates:
        try:
            with open(tickdir + item.strftime(iupac), 'rb') as handle:
                tick = pickle.load(handle)
            handle.close()
            d[item]=tick
        except:
            continue
df=pd.DataFrame(columns=['Date','Timestamp','Volume','OI','ltp'])
start_time=datetime.time(9,16,00)
end_time=datetime.time(15,29,00)
may_start=datetime.date(2021,4,30)
complete_data=[]
for item in ticklist:
    ticktime = datetime.datetime.strptime(item, iupac)
    tickdate = ticktime.date()
    if ticktime.time() > datetime.time(15, 20, 00) and tickdate not in complete_data and tickdate>may_start:
        complete_data.append(tickdate)
        print(tickdate)
print(complete_data)
nthreads=0


for item in ticklist:
    ticktime = datetime.datetime.strptime(item, iupac)
    tickdate  = ticktime.date()
    if tickdate not in complete_data:
        continue
    # print(item, len(df))
    ticktime = datetime.datetime.strptime(item, iupac)
    tickdate = ticktime.date()
    if start_time < ticktime.time() < end_time:
        try:
            with open(tickdir + item, 'rb') as handle:
                tick = pickle.load(handle)
                if st['NIFTY21MAYFUT'] in tick.keys():
                    tick = tick[st['NIFTY21MAYFUT']]
                elif st['NIFTY21JUNFUT'] in tick.keys():
                    tick = tick[st['NIFTY21JUNFUT']]
                else:
                    continue
            handle.close()
            nr = {'Date': ticktime.date(), 'Timestamp': ticktime, 'Volume': tick['volume'], 'OI': tick['oi'],
                  'ltp': tick['last_price']}
            df = df.append(nr, ignore_index=True)
        except:
            continue

df['OIC']=df['OI']-df['OI'].shift(1)
df['PC']=df['ltp']-df['ltp'].shift(1)
df['Vol_traded']=df['Volume']-df['Volume'].shift(1)
df['pc/oic']=df['PC']/df['OIC']
df['pure']=df['OIC']/df['Vol_traded']
df.drop(df.loc[df['OIC']==0].index, inplace=True)

df.to_csv('oi.csv',index=False)

df=pd.read_csv('oi.csv')

df=df.dropna()
df.reset_index(drop=1)
summary=[]
for i in range(0,len(df)):
    if df['OIC'][i]>0:
        if df['PC'][i]>0:
            summary.append('Market Longs')
        else:
            summary.append('Limit Longs')
    else:
        if df['PC'][i]>0:
            summary.append('Shorts Stopped out vs Old Longs')
        else:
            summary.append('Longs Stopped out vs Old Shorts')

df['summary']=summary












