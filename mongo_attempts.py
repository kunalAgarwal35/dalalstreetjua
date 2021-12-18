import datetime
import pymongo
import os
import pandas as pd
import requests
import xlwings as xw

file_path = 'xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['rubberband']

client = pymongo.MongoClient("mongodb://localhost:27017/")

# Database Name
db = client["tick_data_csv"]

connection_names = db.connection_names

def date_to_connectionname(date,deritvative_type):
    if deritvative_type == "FUT":
        con_name = date.strftime("%b_%Y").upper() + '_futures'
    elif deritvative_type == "OPT":
        con_name = date.strftime("%b_%Y").upper() + '_options'
    else:
        print("Error: Derivative type not recognized")
        return None
    return con_name

def get_dataframes(symbol_list, date):
    # Retrieve data from MongoDB
    # Merge future 1 and future 2 OI
    # Returm dataframe wits ts as index and oi,ltp as columns

    pd.set_option('mode.chained_assignment', None)
    col = db[date_to_connectionname(date,"FUT")]
    future_months = [1,2]
    # Get filtered data
    x = col.find({'sym': {"$in":symbol_list},'f_m':{"$in":future_months}},{'sym':1,'f_m':1,'ltp':1,'oi':1,'ts':1,'ltq':1,'_id':0})
    df = pd.DataFrame(list(x))
    fut_dfs = {}
    for symbol in list(set(df['sym'].to_list())):
        df_symbol = df[df['sym'] == symbol]
        df1 = df_symbol[df['f_m'] == 1]
        df2 = df_symbol[df['f_m'] == 2]
        df1.set_index('ts',inplace=True,drop=True)
        df2.set_index('ts',inplace=True,drop=True)
        df1.sort_index(inplace=True)
        df2.sort_index(inplace=True)
        # merge on ts
        df_merged = df1.merge(df2,how='outer',left_index=True,right_index=True)
        # Removing first nan values from df_merged
        df_merged = df_merged[(df_merged['oi_x']>0).to_list().index(1):]
        # Fill Empty oi_y values with previous oi_y value
        df_merged.fillna(method='ffill', inplace=True)
        # Add ing new column for oi_x + oi_y
        df_merged['oi'] = df_merged['oi_x'] + df_merged['oi_y']
        df_merged['ltp'] = df_merged['ltp_x']
        df_merged['qty'] = df_merged['ltq_x']
        df_merged = df_merged[['oi','ltp']]
        fut_dfs[symbol] = df_merged
    return fut_dfs
symbol_list = pd.read_csv("data.csv")['symbols'].to_list()
fut_dfs = get_dataframes(symbol_list,datetime.date(2021,9,27))

def calculate_notional_share(fut_dfs):
    # Calculate notional share for each symbol
    notional_share = {}
    for symbol in fut_dfs.keys():
        df = fut_dfs[symbol]
        df['notional_share'] = df['oi'] * df['ltp']
        notional_share[symbol] = df['notional_share']
    return notional_share
notional = calculate_notional_share(fut_dfs)

# set of all timestamps in all symbols of notional
master_list = []
for symbol in notional.keys():
    master_list.extend(notional[symbol].index.to_list())
master_list = list(set(master_list))
master_list.sort()
for symbol in notional.keys():
    notional[symbol] = notional[symbol][~notional[symbol].index.duplicated(keep='last')]
# merge all dataframes in notional
df_master = pd.DataFrame()
df_master['ts'] = master_list
df_master.set_index('ts',inplace=True,drop=True)

for symbol in notional.keys():
    notional[symbol] = notional[symbol].reindex(df_master.index).fillna(method='ffill')


for key,value in notional.items():
    df_master[key] = value

# Remove rows where any column has empty value
df_master = df_master[df_master.notnull().all(axis=1)]
for column in df_master.columns:
    #Converting to billion rs
    df_master[column] = df_master[column]/100000000

df_master['total_notional'] = df_master.sum(axis=1)
df_master.sort_index(inplace=True)
for symbol in notional.keys():
    df_master[symbol] = df_master[symbol]*100/df_master['total_notional']

# df_master['total_notional'].plot()
df_master = df_master[df_master.index.time >= datetime.time(9,30)]
# s.range('A1').value = df_master
df_print = df_master.copy()
#dropping nifty and total notional
df_print.drop(['NIFTY','total_notional'],axis=1,inplace=True)
# Drop bottom 40 % columns
df_print = df_print[df_print.sum(axis=0).sort_values(ascending=False).index[:int(len(df_print.columns)*0.1)]]
import seaborn
import matplotlib.pyplot as plt
import numpy as np
# plot all symbols agains time
for symbol in df_print.columns:
    plt.plot(df_print.index,df_print[symbol],label=symbol)
    plt.legend(loc='right')
# plt.xticks(df_print.index,rotation=90)
# plt.yticks(np.arange(0,1.1,0.1))
plt.show()



