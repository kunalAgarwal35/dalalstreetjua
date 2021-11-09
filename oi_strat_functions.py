import pandas as pd
import numpy as np
import pickle
import datetime
import configparser
import os


oidf = {}
trading = {}
ts = {}
# populate these dicts before running
nse_sizing = {}

def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename, 'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
    return d


lot_size_dict = get_lot_size_dict('April_kite.instruments')
may_lot_size_dict = get_lot_size_dict('May.instruments')
for token in may_lot_size_dict.keys():
    lot_size_dict[token] = may_lot_size_dict[token]



start_time = datetime.time(9, 16, 00)
end_time = datetime.time(15, 30, 00)
ticksdir = 'oldticks/'
ticklist = os.listdir(ticksdir)
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in ticklist]
# =IF(A2="buy",-1*E2,E2)



tdays=[]
ticknumber={}
def missing_ticks():
    for day in tdays:
        for item in dates_list:
            # try:
            if item.date() == day and item.time() > datetime.time(15,20,00) and item.time() < end_time:
                if item.date() not in ticknumber.keys():
                    ticknumber[item.date()]=0
                else:
                    ticknumber[item.date()] = ticknumber[item.date()] + 1
for item in dates_list:
    if item.date() not in tdays:
        tdays.append(item.date())
missing_ticks()

# for day in ticknumber.keys():
#     print(day)
#     for item in dates_list:
#         # try:
#         if item.date() == day and item.time() > datetime.time(15, 20, 00) and item.time() < end_time:
#             with open(ticksdir + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
#                 tick = pickle.load(handle)
#             handle.close()
#             nr={'timestamp':item}

oicdf={}


for item in dates_list[dates_list.index(item):]:
    print(item)
    try:
        if item.date() in ticknumber.keys() and item.time() > datetime.time(9, 15, 10) and item.time() < end_time:
            with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                tick = pickle.load(handle)
            handle.close()
    except Exception as E:
        print(E)
        continue
        for key in tick.keys():
            try:
                if key not in oicdf.keys():
                    oicdf[key]=pd.DataFrame(columns=['timestamp','oi','ltp'])
                    nr = {'timestamp':item,'oi':tick[key]['oi'],'ltp':tick[key]['last_price']}
                    oicdf[key] = oicdf[key].append(nr, ignore_index=True)
                else:
                    nr = {'timestamp': item, 'oi': tick[key]['oi'], 'ltp': tick[key]['last_price']}
                    if oicdf[key]['oi'].iloc[-1] != nr['oi']:
                        oicdf[key] = oicdf[key].append(nr, ignore_index=True)
            except:
                continue

#
for ket in oicdf.keys():
    oicdf[ket]['%oic'] = oicdf[ket]['oi'].pct_change()
    oicdf[ket]['%ltp'] = oicdf[ket]['ltp'].pct_change()
    # oicdf[ket]['d(oi)/dt'] = (oicdf[ket]['oi']-oicdf[ket]['oi'].shift(1))/((oicdf[ket]['timestamp']-oicdf[ket]['timestamp'].shift(1)).dt.total_seconds())
    # oicdf[ket]['25 SMA d(oi)/dt'] = oicdf[ket]['d(oi)/dt'].rolling(25).mean()
    # oicdf[ket]['100 SMA d(oi)/dt'] = oicdf[ket]['d(oi)/dt'].rolling(100).mean()
    # oicdf[ket]['d(100SMA)/dt'] = (oicdf[ket]['100 SMA d(oi)/dt']-oicdf[ket]['100 SMA d(oi)/dt'].shift(1))/((oicdf[ket]['timestamp']-oicdf[ket]['timestamp'].shift(1)).dt.total_seconds())
    # oicdf[ket]['d(ltp)/dt'] = (oicdf[ket]['ltp'] - oicdf[ket]['ltp'].shift(1)) / (
    #     (oicdf[ket]['timestamp'] - oicdf[ket]['timestamp'].shift(1)).dt.total_seconds())
    # oicdf[ket]['d(ltp)/doi'] = (oicdf[ket]['ltp'] - oicdf[ket]['ltp'].shift(1)) / (
    #     (oicdf[ket]['oi'] - oicdf[ket]['oi'].shift(1)))
    # oicdf[ket]['25 SMA d(ltp)/dt'] = oicdf[ket]['d(ltp)/dt'].rolling(25).mean()
    # oicdf[ket]['25 SMA d(ltp)/doi'] = oicdf[ket]['d(ltp)/doi'].rolling(25).mean()
    oicdf[ket].to_csv('oidf/'+ts[ket]+'.csv',index=False)




