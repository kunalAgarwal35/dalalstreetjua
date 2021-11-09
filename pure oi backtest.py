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
end_time=datetime.time(13,11,00)
may_start=datetime.date(2021,4,30)
complete_data=[]
for item in ticklist:
    ticktime = datetime.datetime.strptime(item, iupac)
    tickdate = ticktime.date()
    if ticktime.time() > datetime.time(15, 20, 00) and tickdate not in complete_data and tickdate>may_start:
        complete_data.append(tickdate)
        print(tickdate)
print(complete_data)
# nthreads=0


# for item in ticklist:
#     ticktime = datetime.datetime.strptime(item, iupac)
#     tickdate  = ticktime.date()
#     if tickdate not in complete_data:
#         continue
#     if start_time < ticktime.time() < end_time:
#         try:
#             with open(tickdir + item, 'rb') as handle:
#                 tick = pickle.load(handle)
#                 if st['NIFTY21MAYFUT'] in tick.keys():
#                     tick = tick[st['NIFTY21MAYFUT']]
#                 elif st['NIFTY21JUNFUT'] in tick.keys():
#                     tick = tick[st['NIFTY21JUNFUT']]
#                 else:
#                     continue
#             handle.close()
#             nr = {'Date': ticktime.date(), 'Timestamp': ticktime, 'Volume': tick['volume'], 'OI': tick['oi'],
#                   'ltp': tick['last_price']}
#             df = df.append(nr, ignore_index=True)
#         except:
#             continue
#
# df['OIC']=df['OI']-df['OI'].shift(1)
# df['PC']=df['ltp']-df['ltp'].shift(1)
# df['Vol_traded']=df['Volume']-df['Volume'].shift(1)
# df['pc/oic']=df['PC']/df['OIC']
# df['pure']=df['OIC']/df['Vol_traded']
# df.drop(df.loc[df['OIC']==0].index, inplace=True)
#
# df.to_csv('oi.csv',index=False)

# df=pd.read_csv('oi.csv')
#
# df=df.dropna()
# df.reset_index(drop=1)
# summary=[]
# for i in range(0,len(df)):
#     if df['OIC'][i]>0:
#         if df['PC'][i]>0:
#             summary.append('Market Longs')
#         else:
#             summary.append('Limit Longs')
#     else:
#         if df['PC'][i]>0:
#             summary.append('Shorts Stopped out vs Old Longs')
#         else:
#             summary.append('Longs Stopped out vs Old Shorts')

# df['summary']=summary

def backtest(day,purethres_entry,purethres_exit,oic_thres_entry,oic_thres_exit):
    ledger=pd.DataFrame(columns=['timestamp','type','token','price','lot_size'])
    dfs={}
    intrade={}
    for item in ticklist:
        ticktime = datetime.datetime.strptime(item, iupac)
        tickdate = ticktime.date()
        if tickdate==day and start_time < ticktime.time() < end_time:
            try:
                # print(item)
                with open(tickdir + item, 'rb') as handle:
                    tick = pickle.load(handle)
                handle.close()
                for token in tick.keys():
                    try:
                        if token not in dfs.keys():
                            intrade[token]=0
                            dfs[token]=pd.DataFramkuna e(columns=['timestamp','ltp','oi','volume','bid','ask','oic','pc','pure','buy_qty','sell_qty','oi_high','oi_low','change'])
                            nr={'timestamp':ticktime,'oi':tick[token]['oi'],'ltp':tick[token]['last_price'],'volume':tick[token]['volume'],
                                'bid':tick[token]['depth']['buy'][0]['price'],'ask':tick[token]['depth']['sell'][0]['price'],
                                'oic':0,'pc':0,'pure':0,'buy_qty':tick[token]['buy_quantity'],'sell_qty':tick[token]['sell_quantity'],
                                'oi_high':tick[token]['oi_day_high'],'oi_low':tick[token]['oi_day_low'],'change':tick[token]['change']}
                            dfs[token]=dfs[token].append(nr,ignore_index=True)
                        else:
                            oic=tick[token]['oi']-dfs[token]['oi'].iloc[-1]
                            if oic!=0:
                                pc=tick[token]['last_price']-dfs[token]['ltp'].iloc[-1]
                                pure=oic/(tick[token]['volume']-dfs[token]['volume'].iloc[-1])
                                nr = {'timestamp': ticktime, 'oi': tick[token]['oi'],'ltp':tick[token]['last_price'], 'volume': tick[token]['volume'],
                                      'bid': tick[token]['depth']['buy'][0]['price'], 'ask': tick[token]['depth']['sell'][0]['price'],
                                      'oic': oic, 'pc': pc, 'pure': pure, 'buy_qty': tick[token]['buy_quantity'],
                                      'sell_qty': tick[token]['sell_quantity'],
                                      'oi_high': tick[token]['oi_day_high'], 'oi_low': tick[token]['oi_day_low'],
                                      'change': tick[token]['change']}
                                dfs[token] = dfs[token].append(nr, ignore_index=True)
                                if not intrade[token]:
                                    if 100*np.log(nr['oi']/dfs[token]['oi'].iloc[-2])>oic_thres_entry and pure>purethres_entry:
                                        led = {'timestamp':ticktime, 'type': 'buy', 'token': token,
                                        'price': nr['ask'], 'lot_size': lot_size_dict[token]}
                                        ledger=ledger.append(led,ignore_index=True)
                                        intrade[token]=1
                                else:
                                    if 100*np.log(nr['oi']/dfs[token]['oi'].iloc[-2])<oic_thres_exit and pc<0:
                                        led = {'timestamp': ticktime, 'type': 'sell', 'token': token,
                                               'price': nr['bid'], 'lot_size': lot_size_dict[token]}
                                        ledger = ledger.append(led, ignore_index=True)
                                        intrade[token]=0
                    except Exception as E:
                        # print(E)
                        pass
            except Exception as E:
                # print(E)
                pass
        elif tickdate==day and end_time < ticktime.time():
            # print("Squaring off all positions")
            openpos=0
            for token in intrade.keys():
                if intrade[token]!=0:
                    openpos+=1
            if openpos==0:
                continue
            else:
                print(item)
                with open(tickdir + item, 'rb') as handle:
                    tick = pickle.load(handle)
                handle.close()
                for token in tick.keys():
                    if intrade[token]:
                        led = {'timestamp': ticktime, 'type': 'sell', 'token': token,
                               'price': tick[token]['depth']['buy'][0]['price'], 'lot_size': lot_size_dict[token]}
                        ledger = ledger.append(led, ignore_index=True)
                        intrade[token] = 0
    qty=[]
    tradingsymbols=[]
    for i in range(0,len(ledger)):
        tradingsymbols.append(ts[ledger['token'][i]])
        if ledger['type'][i]=='buy':
            qty.append(-1*ledger['lot_size'][i])
        else:
            qty.append(ledger['lot_size'][i])
    print(day)
    ledger['symbol']=tradingsymbols
    ledger['qty']=qty
    ledger['bal']=ledger['qty']*ledger['price']
    print("Net Qty: ",ledger['qty'].sum())
    print("Net P/L: ", ledger['bal'].sum())
    print("Average: ",ledger['bal'].mean())
    ledger.to_csv('ledger2/'+day.strftime("%m-%d-%Y")+'.csv',index=False)

for day in complete_data:
    backtest(day,0.3,-0.5,5,-0.2)
ledgers=[]
for led in os.listdir('ledger2'):
    ledgers.append(pd.read_csv('ledger2/'+led))

result=pd.concat(ledgers,ignore_index=True)

print("All Tokens Net: ",result['bal'].sum())
print("All Tokens Mean: ",result['bal'].mean())




# day=complete_data[1]
# purethres_entry,purethres_exit,oic_thres_entry,oic_thres_exit=0.6,-0.4,0,0.2















