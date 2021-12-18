import math
import random
import datetime
import pandas as pd
import pickle
import numpy as np
import function_store2 as fs
import xlwings as xw
import time
import gdfl
#
file_path = 'xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['rubberband']

# instrument = 'TATAMOTORS'
# expiries = gdfl.find_expiries(instrument)
# expiries.sort()
#
# expiry = expiries[10]
# df = gdfl.opt_ltps(instrument, expiry)
# ltps,ois,bidasks = df[0],df[1],df[2]
# df = ''
#
def price_by_options(ltps):
    opt_prices = {}
    for timestamp in ltps.keys():
        opt_prices[timestamp] = []
        for option in ltps[timestamp].keys():
            if 'CE' in option:
                price = float(option.replace('CE','')) + float(ltps[timestamp][option])
                opt_prices[timestamp].append(price)
            elif 'PE' in option:
                price = float(option.replace('PE','')) - float(ltps[timestamp][option])
                opt_prices[timestamp].append(price)
        opt_prices[timestamp] = float(pd.Series(opt_prices[timestamp]).mean())
    # Sort the dictionary by key
    opt_prices = dict(sorted(opt_prices.items()))
    return opt_prices

# opt_prices = price_by_options(ltps)

# tata_fut = gdfl.get_fut_ltp_df(instrument, expiry,expiries[expiries.index(expiry)-1])
# tata_fut_ltps = tata_fut['LTP']
# tata_fut_dates = tata_fut['Date']
# tata_fut_times = tata_fut['Time']
# tata_fut_datetime = [datetime.datetime.combine(datetime.datetime.strptime(date,'%d/%m/%Y').date(), datetime.datetime.strptime(time,'%H:%M:%S').time()) for date, time in zip(tata_fut_dates, tata_fut_times)]
# tata_fut_datetime = [i for i in tata_fut_datetime if i in opt_prices.keys()]
# fut_prices = dict(zip(tata_fut_datetime, tata_fut_ltps))
# # sort fut_prices by key
# fut_prices = dict(sorted(fut_prices.items()))
# compare_df = pd.DataFrame()
# for timestamp in fut_prices.keys():
#     if timestamp in opt_prices.keys():
#         compare_df = compare_df.append({'timestamp':timestamp,'fut_price':fut_prices[timestamp], 'opt_price':opt_prices[timestamp]}, ignore_index=True)
#
# # plot compare_df
# compare_df['diff'] = compare_df['fut_price'] - compare_df['opt_price']
# compare_df['diff'].plot()
# s.range('A1').value = compare_df
# print(gdfl.instruments)
futures = {}
for instrument in gdfl.instruments:
    try:
        t1 = time.time()
        expiries = gdfl.find_expiries(instrument)
        expiries.sort()
        futures[instrument] = gdfl.get_fut_ltp_df(instrument,expiries[len(expiries)-1],expiries[len(expiries)-2])
        print(instrument, time.time()-t1)
    except:
        continue

def get_distribution(price,df,stop,target,range_):
    n_df = df.copy()
    # converting to datetime
    try:
        n_df['Date'] = [datetime.datetime.combine(datetime.datetime.strptime(date,'%d/%m/%Y').date(), datetime.datetime.strptime(time,'%H:%M:%S').time()) for date, time in zip(n_df['Date'], n_df['Time'])]
    except TypeError:
        n_df['Date'] = [datetime.datetime.combine(datetime.datetime.strptime(date, '%d/%m/%Y').date(),time) for date, time in zip(n_df['Date'], n_df['Time'])]
    # Filtering when price is in range
    n_df['in_range'] = [True if price-range_<=n_df['LTP'][i]<=price+range_ and i else False for i in range(len(n_df))]
    # removing true where price was in range in last row
    n_df['in_range_first'] = [False if n_df['in_range'][i]==False or n_df['in_range'][i-1]==True else True for i in range(len(n_df))]
    # timestamps when price is in range
    timestamps = [n_df['Date'][i] for i in range(len(n_df)) if n_df['in_range_first'][i]==True]
    # checking if target hit before stop in timestamps
    target_count, stop_count = 0, 0

    for timestamp in timestamps:
        if 'last_ts' in locals():
            if timestamp < last_ts:
                continue
        sub_df = n_df[n_df['Date']>=timestamp]
        sub_df.reset_index(drop=True, inplace=True)
        for i in range(len(sub_df)):
            if sub_df['LTP'][i]>=price + target:
                target_count += 1
                last_ts = sub_df['Date'][i]
                print('Target: ', timestamp, last_ts, ' Price: ',price,' Target: ',price+target,' Stop: ',price-stop)
                break
            if sub_df['LTP'][i]<=price - stop:
                stop_count += 1
                last_ts = sub_df['Date'][i]
                print('Stop: ', timestamp, last_ts,' Price: ',price,' Target: ',price+target,' Stop: ',price-stop)
                break
    print(price, target_count, stop_count)
    return {'price':price,'target':target_count,'stop':stop_count}

def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier
# results = {}
# for instrument in gdfl.instruments:
#     try:
#         df = futures[instrument]
#         prices = random.sample(df['LTP'].to_list(),5)
#         res = {}
#         for price in prices:
#             res[price] = get_distribution(price,df,stop=price*0.01,target=price*0.02,range_=round_down(price*0.0001,1))
#         results[instrument] = res
#     except:
#         continue

# expiries

# result_df = pd.DataFrame()
# for instrument in results.keys():
#     for price in results[instrument].keys():
#         result_df = result_df.append({'Location':instrument +' '+ str(price), 'Targets':results[instrument][price]['target'],'Stops':results[instrument][price]['stop']},ignore_index=True)

# result_df['Trades'] = result_df['Targets'] + result_df['Stops']
# result_df['Accuracy'] = result_df['Targets']*100/(result_df['Trades'])
# result_df.sort_values(by='Accuracy',ascending=False,inplace=True)
# s.range('A1').value = result_df

def find_important_levels(instrument):
    df = futures[instrument]
    prices = np.arange(round_down(df['LTP'].min(),2),round_down(df['LTP'].max(),2),round_down(df['LTP'].max()*.001,2))
    res = {}
    for price in prices:
        res[price] = get_distribution(price,df,stop=round_down(price*(0.01),2),target=round_down(price*(0.025),2),range_=round_down(price*0.0001,2))
    result_df = pd.DataFrame()
    for price in res.keys():
        result_df = result_df.append({'Location':instrument +' '+ str(price), 'Targets':res[price]['target'],'Stops':res[price]['stop']},ignore_index=True)
    result_df['Trades'] = result_df['Targets'] + result_df['Stops']
    result_df['Accuracy'] = result_df['Targets']*100/(result_df['Trades'])
    result_df.sort_values(by='Accuracy',ascending=False,inplace=True)
    return result_df

# s.range('A1').value = find_important_levels('ADANIPOWER')


instruments = gdfl.instruments
