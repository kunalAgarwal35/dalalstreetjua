import pandas as pd
import os
import function_store as fs
import pickle
import dateutil
import datetime
import time
import xlwings as xw
import zd
import function_store as fs
import statistics
import numpy as np
file_path = 'xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['temperature (4)']

# histdict = os.path.join('kitehistorical','options','NIFTY')
# print(os.listdir(histdict))
# for exp in os.listdir(histdict):
#     contracts = {}
#     for file in os.listdir(os.path.join(histdict,exp)):
#         contracts[file.replace('.csv','')] = pd.read_csv(os.path.join(histdict,exp,file))
#     break
'''
parameters to calculate for every x days:
    1. high - close (%)
    2. high - low (%)
    3. close - open (%)
    4. bullish % at open (use option vodoo)

simple momentum
    1. get ndis mean and sd for last 6 months for nifty 150    
'''



def nifty250EQ_tokens():
    csv = pd.read_csv("nifty250.csv")
    symbols = list(csv['Symbol'])
    tokens = {}
    types = ['FUT']
    expfut = 100
    instru = zd.kite.instruments()
    now = datetime.datetime.now()
    futures = []
    for ins in instru:
        if ins['instrument_type'] == 'FUT':
            futures.append(ins['name'])
    for ins in instru:
        if ins['exchange'] == 'NSE' and ins['tradingsymbol'] in symbols and ins['tradingsymbol'] in futures:
            tokens[ins['instrument_token']] = ins['tradingsymbol']
    return tokens

n250 = nifty250EQ_tokens()
trade_date = datetime.datetime(2020,1,1)

def get_all_historical_ohlc(from_date):
    historical_ohlc = {}
    for token in n250.keys():
        if token not in historical_ohlc.keys():
            try:
                historical_ohlc[token] = zd.get_historical(token, from_date, datetime.datetime.now().date(), "5minute", 0)
                print('Stored ',n250[token])
            except:
                print('Error in ',n250[token])
    return historical_ohlc

def ret_df(trade_date,past_n_days,sq_off_days,historical_ohlc_all):
    historical_ohlc = {}
    from_date = trade_date - datetime.timedelta(days = past_n_days)
    sq_off_date = trade_date + datetime.timedelta(days = sq_off_days)
    ret_ohlc = {}
    for token in historical_ohlc_all.keys():
        d = historical_ohlc_all[token]
        historical_ohlc[token] = d[from_date < d['date']]
        historical_ohlc[token] = historical_ohlc[token][historical_ohlc[token]['date'] <= trade_date]
        ret_ohlc[token] = d[trade_date < d['date']]
        ret_ohlc[token] = ret_ohlc[token][ret_ohlc[token]['date'] <= sq_off_date]
    trading_sessions = 75*sq_off_days
    dist = {}
    for token in historical_ohlc.keys():
        dist[token] = fs.stock_distribution(trading_sessions,historical_ohlc[token])
    df = pd.DataFrame()
    for token in dist.keys():
        try:
            rr = ret_ohlc[token]['close']
            nr = {'instrument':n250[token],'mean':statistics.mean(dist[token]),
                  'sd':statistics.stdev(dist[token])
                  }
            try:
                nr['ret'] = 100 * np.log(rr.iloc[-1] / rr.iloc[0])
            except:
                nr['ret'] = 0
            nr['mean/sd'] = nr['mean']/nr['sd']
            df = df.append(nr,ignore_index=True)
        except:
            print('DistError ',n250[token])

    df = df.sort_values(by = 'mean/sd',ascending=False)
    return df


nifty = pickle.load(open('nifty_historical.pkl','rb'))
average_returns = pd.DataFrame()
past_n_days = 300
past_n_days2 = 30

sq_off_days = 30
#Number of stocks to keep in portfolio
top_x = 10
from_date = datetime.datetime(2017,1,1).date()
historical_ohlc_all = get_all_historical_ohlc(from_date)
sim_start_date = datetime.date(2017,2,1)
trading_dates = nifty['date'][nifty['date'] > datetime.datetime(sim_start_date.year,sim_start_date.month,sim_start_date.day) + datetime.timedelta(days = past_n_days+5)]
trading_dates = [datetime.datetime(n.year,n.month,n.day,9,20,0) for n in trading_dates]
trading_dates = list(set(trading_dates))
trading_dates = sorted(trading_dates)
traded_months = []
for date in trading_dates:
    # if str(date.month)+str(date.year) in traded_months:
    #     continue
    # else:
    #     traded_months.append(str(date.month)+str(date.year))
    df = pd.DataFrame()
    df = ret_df(date, past_n_days, sq_off_days, historical_ohlc_all)
    df2 = ret_df(date, past_n_days2, sq_off_days, historical_ohlc_all)
    lst1 = df['instrument'].to_list()[:top_x]
    lst2 =df2['instrument'].to_list()[:top_x]
    intersection = list(set(lst1) & set(lst2))
    ret_list = []
    stocks = []
    for stock in intersection:
        ret_list.append(float(df['ret'][df['instrument'] == stock]))
        stocks.append(stock)
    if not len(ret_list):
        ret_list.append(0)
    nnifty = nifty[nifty['date'] >= date]
    nnifty = nnifty[nnifty['date'] <= date + datetime.timedelta(days = sq_off_days)]
    nr = {'date': date, 'return': statistics.mean(ret_list),
          'index_return': 100 * np.log(nnifty['close'].iloc[-1] / nnifty['close'].iloc[0]),
          'trades':' '.join([str(elem) for elem in stocks])}
    average_returns = average_returns.append(nr, ignore_index=True)
    average_returns['cum_ind'] = average_returns['index_return'].cumsum()
    average_returns['cum_str'] = average_returns['return'].cumsum()

    s.range('A1').value = average_returns
# for year in range(2019,2022):
#     for month in range(1,12):
#         print(datetime.datetime(year,month,1))
#         df = pd.DataFrame()
#         df = ret_df(datetime.datetime(year,month,1),past_n_days,sq_off_days,historical_ohlc_all)
#         nnifty = nifty[nifty['date'] >= datetime.datetime(year,month,1)]
#         nnifty = nnifty[nnifty['date'] <= datetime.datetime(year,month,1) + datetime.timedelta(sq_off_days)]
#         nr = {'date': datetime.datetime(year,month,1), 'return': statistics.mean(list(df['ret'])[:top_x]),
#            'index_return': 100*np.log(nnifty['close'].iloc[-1]/nnifty['close'].iloc[0])}
#         average_returns = average_returns.append(nr,ignore_index=True)
#         s.range('A1').value = average_returns
# for token in n250.keys():
#     print(n250[token],token)

sq_off_days = 30
df300 = ret_df(datetime.datetime.now(),past_n_days,sq_off_days,historical_ohlc_all)
df30 = ret_df(datetime.datetime.now(),200,sq_off_days,historical_ohlc_all)
l1 = df300['instrument'].to_list()[:10]
l2 = df30['instrument'].to_list()[:10]
for item in l1:
    if item in l2:
        print(item)
        print(l1.index(item),l2.index(item))

#
# niktick = {}
# for timestamp in ticks.keys():
#     if timestamp not in niktick.keys():
#         niktick[timestamp] = {}
#         print(timestamp)
#     tick = ticks[timestamp]
#     for item in tick.keys():
#         if tick[item]['instrument_type'] == 'FUT':
#             if 'NIFTY' in tick[item]['name'] or 'SBI' in tick[item]['name']:
#                 niktick[timestamp][item] = tick[item]
#
# pickle.dump(niktick,open('E:/OneDrive/Shared/Tickdata/niktick.pkl','wb'))