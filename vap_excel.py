import xlwings as xw
import pandas as pd
import pickle, os
import numpy as np
import datetime
import time
file_path='xlwings.xlsx'
tickdir='juneticks/'
newtickdir='ticks/'
iupac="%m%d%Y-%H%M%S-%f"
ticklist = os.listdir(tickdir)
dates_list = [datetime.datetime.strptime(date, iupac) for date in ticklist]
tradingsymbol='NIFTY21JUNFUT'
start_time=datetime.time(9,15,10)
end_time=datetime.time(15,30,00)
ts={}
def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename, 'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
    return d
lot_size_dict = get_lot_size_dict('june.instruments')
st={}
for key in ts.keys():
    st[ts[key]]=key

# niftyvap=pd.DataFrame(columns=["Price","Volume"])
# niftyvap['Price'] = np.arange(15000,16000.05,0.05).round(decimals=2).tolist()
# niftyvap=niftyvap.set_index('Price')
# niftyvap['Volume'] = niftyvap['Volume'].fillna(0)
# df=pd.DataFrame(columns=['ltp','volume'])
# for item in dates_list:
#     if start_time < item.time() < end_time:
#         try:
#             with open(tickdir + item.strftime(iupac), 'rb') as handle:
#                 tick = pickle.load(handle)[st[tradingsymbol]]
#             handle.close()
#         except:
#             continue
#         nr={'ltp':tick['last_price'],'volume':tick['volume']}
#         if len(df)>2:
#             rangeofprice=np.arange(min(df['ltp'].iloc[-1],nr['ltp']),max(df['ltp'].iloc[-1],nr['ltp'])+0.05,0.05).round(decimals=2).tolist()
#             unitvol=(nr['volume']-df['volume'].iloc[-1])/len(rangeofprice)
#             for price in rangeofprice:
#                 niftyvap.loc[price,'Volume']=niftyvap['Volume'].loc[price]+unitvol
#         df=df.append(nr,ignore_index=True)
#         xw.Book(file_path).sheets['Sheet1'].range('A1').value = niftyvap


# niftyvap.to_csv('vaps/niftyvap.csv')
niftyvap=pd.read_csv('vaps/niftyvap.csv')
niftyvap=niftyvap.set_index('Price')
niftyvap['today']=0
df=pd.DataFrame(columns=['ltp','volume'])
processedlist=[]
while(1==1):
    newticklist = os.listdir(newtickdir)
    new_dates_list=[datetime.datetime.strptime(date, iupac) for date in newticklist]
    for item in new_dates_list:
        if start_time < item.time() < end_time and item not in processedlist:
            print(item)
            try:
                with open(newtickdir + item.strftime(iupac), 'rb') as handle:
                    tick = pickle.load(handle)[st[tradingsymbol]]
                handle.close()
            except:
                continue
            nr = {'ltp': tick['last_price'], 'volume': tick['volume']}
            if len(df) > 2:
                rangeofprice = np.arange(min(df['ltp'].iloc[-1], nr['ltp']), max(df['ltp'].iloc[-1], nr['ltp']) + 0.05,
                                         0.05).round(decimals=2).tolist()
                unitvol = (nr['volume'] - df['volume'].iloc[-1]) / len(rangeofprice)
                for price in rangeofprice:
                    niftyvap.loc[price, 'today'] = niftyvap['today'].loc[price] + unitvol
            df = df.append(nr, ignore_index=True)
            processedlist.append(item)
    xw.Book(file_path).sheets['Sheet1'].range('A1').value = niftyvap
    time.sleep(0.5)











