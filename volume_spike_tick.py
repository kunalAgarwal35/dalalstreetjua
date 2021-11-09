import datetime
import pickle, os
import pandas as pd
import numpy as np
import datetime
import xlwings as xw
dateformat="%y-%m-%d"
day_tick_dir='dayticks/'
day_list=[i for i in os.listdir(day_tick_dir) if os.stat(day_tick_dir+i).st_size/(1024*1024)>1400]
day_list=[datetime.datetime.strptime(i,dateformat) for i in day_list]
day_list=[i for i in day_list if i>datetime.datetime(2021,4,29)]
file_path='xlwings.xlsx'
sheet2=xw.Book(file_path).sheets['Sheet2']
ts={}
def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename, 'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
    return d
lot_size_dict = get_lot_size_dict('May.instruments')
june_lot_size_dict = get_lot_size_dict('june.instruments')
for token in june_lot_size_dict.keys():
    lot_size_dict[token] = june_lot_size_dict[token]
st={}
for key in ts.keys():
    st[ts[key]]=key

file=day_list[0]
with open(day_tick_dir + file.strftime(dateformat), 'rb') as handle:
    ticksdict = pickle.load(handle)
handle.close()

#Filtering out nifty ticks from ticksdict
for date in ticksdict.keys():
    niftymay=st['NIFTY21MAYFUT']
    niftyjun=st['NIFTY21JUNFUT']
    try:
        tick = ticksdict[date][niftymay]
    except:
        tick = ticksdict[date][niftyjun]
    ticksdict[date]=tick

#Converting to dataframe
df=pd.DataFrame()
for date in ticksdict.keys():
    ticksdict[date]['timestamp']=date
    df=df.append(ticksdict[date],ignore_index=True)

df.set_index('timestamp',drop=True,inplace=True)
df.drop(columns=['tradable'])
df['volume_diff']=df['volume']-df['volume'].shift(1)
df['%change_diff']=df['change']-df['change'].shift(1)
df['oic']=df['oi']-df['oi'].shift(1)
df['timestamp']=df.index
secdiff=(df['timestamp']-df['timestamp'].shift(1))
secdiff=[i.seconds for i in secdiff]
df['vol/sec']=df['volume_diff']/secdiff
df['vol_ratio']=df['vol/sec']/df['vol/sec'].mean()
res=['average_price','depth','ohlc','mode','tradable','timestamp']
l=[i for i in df.columns if i not in res]
sheet2.range('A1').options(index=True).value=df[l]








