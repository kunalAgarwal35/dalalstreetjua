import xlwings as xw
import pandas as pd
import pickle, os
import numpy as np
import datetime
import time
file_path='xlwings.xlsx'
sheet1=xw.Book(file_path).sheets['Sheet1']
tickdir='juneticks/'
newtickdir='ticks/'
iupac="%m%d%Y-%H%M%S-%f"
ticklist = os.listdir(tickdir)
# since=datetime.datetime.strptime(sheet1.range('C2').value, iupac)
since=os.listdir('vaps')
since=[datetime.datetime.strptime(date.replace('.csv',''), iupac) for date in since]
since=max(since)
dates_list = [datetime.datetime.strptime(date, iupac) for date in ticklist]
dates_list = [i for i in dates_list if since<i]
tradingsymbol=sheet1.range('A2').value
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


#Initiate niftyvap
# niftyvap=pd.DataFrame(columns=['last_tick_volume','bids','Price','asks','vap today','vap since'])
# niftyvap['Price'] = np.arange(sheet1.range('D2').value,sheet1.range('E2').value,sheet1.range('B2').value).round(decimals=0).tolist()
# niftyvap['price'] = np.arange(sheet1.range('D2').value,sheet1.range('E2').value,sheet1.range('B2').value).round(decimals=0).tolist()
# niftyvap=niftyvap.set_index('price')
# niftyvap['vap since']=0
# niftyvap['vap today']=0
# niftyvap['bids']=0
# niftyvap['asks']=0
# niftyvap['last_tick_volume']=0
# niftyvap.sort_index(ascending=False,inplace=True)

#Load niftyvap
# niftyvap=pd.DataFrame(sheet1.range('A4:F1304').value)
# niftyvap.columns=['last_tick_volume','bids','Price','asks','vap today','vap since']
# niftyvap=niftyvap.drop(0)
# niftyvap['price']=niftyvap['Price']
# niftyvap=niftyvap.set_index('price', drop=True)

#load niftyvap
niftyvap=pd.read_csv('vaps/'+since.strftime(iupac)+'.csv')
niftyvap=niftyvap.set_index('price', drop=True)

df=pd.DataFrame(columns=['ltp','volume'])
def closest(lst, K):
    lst = np.asarray(lst)
    idx = (np.abs(lst - K)).argmin()
    return lst[idx]

for item in dates_list:
    if item.time() > end_time:
        df = pd.DataFrame(columns=['ltp', 'volume'])
    if start_time < item.time() < end_time:
        print(item)
        try:
            with open(tickdir + item.strftime(iupac), 'rb') as handle:
                tick = pickle.load(handle)[st[tradingsymbol]]
            handle.close()
            bids = tick['depth']['buy']
            asks = tick['depth']['sell']
        except:
            continue
        bidprices=[i['price'] for i in bids]
        bidqty = [i['quantity'] for i in bids]
        askprices = [i['price'] for i in asks]
        askqty = [i['quantity'] for i in asks]
        niftyvap.loc[niftyvap.index < bidprices[0], ['asks']] = 0
        niftyvap.loc[niftyvap.index > askprices[0], ['bids']] = 0
        for i in range(0,len(bidprices)):
            niftyvap.loc[closest(niftyvap.index.tolist(),bidprices[i]),'bids']=bidqty[i]
            niftyvap.loc[closest(niftyvap.index.tolist(), askprices[i]), 'asks'] = askqty[i]
        nr={'ltp':tick['last_price'],'volume':tick['volume']}
        if len(df)>2:
            rangeofprice=np.arange(closest(niftyvap.index.tolist(),min(df['ltp'].iloc[-1],nr['ltp'])),
                                   closest(niftyvap.index.tolist(),max(df['ltp'].iloc[-1],nr['ltp'])+sheet1.range('B2').value),
                                   sheet1.range('B2').value).round(decimals=int(sheet1.range('F2').value)).tolist()
            unitvol=(nr['volume']-df['volume'].iloc[-1])/len(rangeofprice)
            niftyvap['last_tick_volume'] = 0
            for price in rangeofprice:
                niftyvap.loc[price,'vap since']=niftyvap['vap since'].loc[price]+unitvol
                niftyvap.loc[int(price), 'last_tick_volume'] = unitvol
        df=df.append(nr,ignore_index=True)
        niftyvap['bids'].replace([0, 0.0], '', inplace=True)
        niftyvap['asks'].replace([0, 0.0], '', inplace=True)
        niftyvap['last_tick_volume'].replace([0, 0.0], '', inplace=True)
        #Send to Excel:
        if not int(sheet1.range('H1').value):
            sheet1.range('A4').options(index=False).value = niftyvap
            sheet1.range('D3').value = tick['last_price']
            sheet1.range('B3').value = item

try:
    niftyvap.to_csv('vaps/'+item.strftime(iupac)+'.csv')
except:
    pass


# niftyvap=pd.DataFrame(columns=['price','vap since','vap today','bid''asks'])
# niftyvap['Price'] = np.arange(sheet1.range('D2').value,sheet1.range('E2').value,sheet1.range('B2').value).round(decimals=0).tolist()
# niftyvap=niftyvap.set_index('Price')
# niftyvap['vap since']=0
# niftyvap['vap today']=0
# niftyvap['bids']=0
# niftyvap['asks']=0
df=pd.DataFrame(columns=['ltp','volume'])
# processedlist=[]

processed_last=datetime.datetime.now().date()
processed_last=datetime.datetime.combine(processed_last,start_time)
while(1==1):
    newticklist = os.listdir(newtickdir)
    new_dates_list=[datetime.datetime.strptime(date, iupac) for date in newticklist]
    new_dates_list=[i for i in new_dates_list if i>processed_last]
    for item in new_dates_list:
        if start_time < item.time() < end_time:
            print(item)
            try:
                with open(newtickdir + item.strftime(iupac), 'rb') as handle:
                    tick = pickle.load(handle)[st[tradingsymbol]]
                handle.close()
                bids = tick['depth']['buy']
                asks = tick['depth']['sell']
            except:
                # processedlist.append(item)
                processed_last=item
                continue
            bidprices=[i['price'] for i in bids]
            bidqty = [i['quantity'] for i in bids]
            askprices = [i['price'] for i in asks]
            askqty = [i['quantity'] for i in asks]
            niftyvap.loc[niftyvap.index > bidprices[4], ['bids']] = 0
            niftyvap.loc[niftyvap.index < askprices[4], ['asks']] = 0
            for i in range(0,len(bidprices)):
                cp=closest(niftyvap.index.tolist(), bidprices[i])
                try:
                    niftyvap.loc[cp,'bids'] = niftyvap['bids'].loc[cp]+bidqty[i]
                except TypeError:
                    niftyvap.loc[cp, 'bids'] = bidqty[i]
                cp = closest(niftyvap.index.tolist(), askprices[i])
                try:
                    niftyvap.loc[cp, 'asks'] = niftyvap['asks'].loc[cp]+askqty[i]
                except TypeError:
                    niftyvap.loc[cp, 'asks'] = askqty[i]
            nr={'ltp':tick['last_price'],'volume':tick['volume']}
            if len(df) > 2:
                rangeofprice = np.arange(closest(niftyvap.index.tolist(), min(df['ltp'].iloc[-1], nr['ltp'])),
                                         closest(niftyvap.index.tolist(),
                                                 max(df['ltp'].iloc[-1], nr['ltp']) + sheet1.range('B2').value),
                                         sheet1.range('B2').value).round(
                    decimals=int(sheet1.range('F2').value)).tolist()
                unitvol = (nr['volume'] - df['volume'].iloc[-1]) / len(rangeofprice)
                niftyvap['last_tick_volume'] = 0
                for price in rangeofprice:
                    niftyvap.loc[price, 'vap today'] = niftyvap['vap today'].loc[price] + unitvol
                    niftyvap.loc[int(price), 'last_tick_volume'] = unitvol
            df = df.append(nr, ignore_index=True)
            niftyvap['bids'].replace([0, 0.0], '', inplace=True)
            niftyvap['asks'].replace([0, 0.0], '', inplace=True)
            niftyvap['last_tick_volume'].replace([0, 0.0], '', inplace=True)
            # Send to Excel:
            if not int(sheet1.range('H1').value):
                sheet1.range('A4').options(index=False).value = niftyvap
                sheet1.range('D3').value = tick['last_price']
                sheet1.range('B3').value = item
            # processedlist.append(item)
            processed_last=item
#
#
#
#
#
#






