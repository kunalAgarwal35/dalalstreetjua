import pandas as pd
import numpy as np
import os
import xlwings as xw
import time
import datetime
import functools
import cProfile,pstats,io
from pstats import SortKey
pr = cProfile.Profile()

from plone.memoize import forever

def fname_to_instrument(fname):
    return fname.split('-')[0]


def fname_to_date(fname):
    return datetime.datetime.strptime(fname.split('_')[2], '%d%m%Y').date()

@forever.memoize
def get_first_month_future_df(date,instrument):
    # get the first month future dataframe
    # date: the first month of the future
    # return: the first month future dataframe
    data_loc = 'E:/tickdata/'
    month_foldername = date.strftime('%b_%Y').upper()
    available_days = [fname_to_date(x) for x in os.listdir(data_loc + month_foldername)]
    if type(date) != type(datetime.datetime(2000, 1, 1).date()):
        try:
            date = date.date()
        except Exception as e:
            print(e)
    if date not in available_days:
        print('Date is not a trading day')
        return
    for day in os.listdir(os.path.join(data_loc, month_foldername)):
        if fname_to_date(day) == date:
            fname = os.path.join(data_loc, month_foldername, day, 'Futures','-I',instrument.upper()+'-I.NFO.csv')
            return pd.read_csv(fname)



@forever.memoize
def get_trading_days(month_foldername):
    # get the trading days of the month
    # month_year: the month and year
    # return: the trading days of the month
    # example: get_trading_days('Jan_2018')
    month_year = month_foldername.upper()
    data_loc = 'E:/tickdata/'
    available_days = [fname_to_date(x) for x in os.listdir(data_loc + month_foldername)]
    return available_days



@forever.memoize
def get_foldernames():
    # get the foldernames of the tick data
    # return: the foldernames of the tick data
    data_loc = 'E:/tickdata/'
    ret_list = []
    excluded_folders = ['saved_files']
    for item in os.listdir(data_loc):
        if os.path.isdir(os.path.join(data_loc, item)) and item not in excluded_folders:
            ret_list.append(item)
    return ret_list



@forever.memoize
def get_nfo_stock_names():
    # get the nifty50 stock names
    # return: the nifty50 stock names
    data_loc = 'E:/tickdata/'
    ret_list = []
    for item in os.listdir(data_loc):
        if os.path.isdir(os.path.join(data_loc, item)) and item not in ['saved_files']:
            for subitem in os.listdir(os.path.join(data_loc, item)):
                if os.path.isdir(os.path.join(data_loc, item, subitem)):
                    for subsubitem in os.listdir(os.path.join(data_loc, item, subitem,'Futures','-I')):
                        if subsubitem.endswith('-I.NFO.csv'):
                            if subsubitem.split('-')[0] not in ret_list:
                                ret_list.append(subsubitem.split('-')[0])

    return ret_list



@forever.memoize
def get_all_trading_days():
    trading_days = []
    for foldername in get_foldernames():
        trading_days = trading_days + get_trading_days(foldername)
    trading_days.sort()
    return trading_days



@forever.memoize
def get_last_n_trading_days(date,n):
    # get the last n trading days
    trading_days = get_all_trading_days()
    trading_days = [i for i in trading_days if trading_days[trading_days.index(date) - n] < i <= date]
    return trading_days


def get_last_n_trading_days_df(date,n,instrument):
    days = get_last_n_trading_days(date,n)
    ret_dfs = {}
    for day in days:
        ret_dfs[day] = get_first_month_future_df(day,instrument)
    return ret_dfs


def constant_volume_distribution(dfs,division_factor):
    # dfs : the dict of dataframes of the last n trading days
    # return: the list of distrbutions of the last n trading days
    # division _factor: the factor to divide the average daily volume by
    ret_dfs = {}
    average_daily_volume = pd.Series([dfs[x]['LTQ'].sum() for x in dfs]).mean()
    tenth_average = average_daily_volume / division_factor
    dist_list = []
    cum_df = pd.DataFrame()
    for df in dfs.keys():
        cum_df = pd.concat([cum_df,dfs[df]],axis=0)

    while cum_df['LTQ'].sum() > tenth_average:
        # print(cum_df['LTQ'].sum())
        open = cum_df['LTP'].iloc[0]
        cum_df['Volume'] = cum_df['LTQ'].cumsum()
        if cum_df['Volume'].iloc[0] > tenth_average:
            cum_df = cum_df[1:]
        else:
            cum_df = cum_df[cum_df['Volume'] > tenth_average]
        close = cum_df['LTP'].iloc[0]
        dist_list.append(100*np.log(close/open))

    return {'mean':np.mean(dist_list),'std':np.std(dist_list)}

def multiple_timeperiod_distribution(dfs,division_factor):
    # dfs : the dict of dataframes of the last n trading days
    # return: the list of distrbutions of the last n trading days
    # division _factor: the factor to divide the average daily volume by
    ret_dfs = {}

    mean_list = []
    std_list = []
    cum_df = pd.DataFrame()
    for df in dfs.keys():
        cum_df = pd.concat([cum_df,dfs[df]],axis=0)

    shifts_list = [600,3600,18000]
    for period in shifts_list:
        cum_df[str(period)] = 100*np.log(cum_df['LTP'].shift(-period)/cum_df['LTP'])
        cum_df = cum_df.dropna()
        mean_list.append(cum_df[str(period)].mean())
        std_list.append(cum_df[str(period)].std())

    return {'mean':np.mean(mean_list),'std':np.mean(std_list)}


def get_all_instruments_distribution(date,n,division_factor):
    ret_dict = {}
    for instrument in get_nfo_stock_names():
        try:
            # print(instrument)
            dfs = get_last_n_trading_days_df(date, n, instrument)
            ret_dict[instrument] = multiple_timeperiod_distribution(dfs,division_factor)
        except Exception as e:
            print('get_all_inistruments_distribution ',e)
            pass
            # print('error in ' + instrument)
    ret_df = pd.DataFrame.from_dict(ret_dict,orient='index')
    return ret_df


def get_next_trading_day_returns(date,instrument):
    trading_days = get_trading_days(date.strftime('%b_%Y').upper())
    if trading_days.index(date) == len(trading_days) - 1:
        trading_days = get_trading_days((date + datetime.timedelta(days=5)).strftime('%b_%Y').upper())
        next_trading_day = trading_days[0]
    else:
        next_trading_day = trading_days[trading_days.index(date) + 1]
    df = get_first_month_future_df(next_trading_day,instrument)
    # To test long strategy
    # entry = pd.Series(df['LTP'].to_list()[5:30]).max()
    # return 100*np.log(df['LTP'].iloc[-1] / entry)

    # To test short strategy
    entry = pd.Series(df['LTP'].to_list()[:30]).min()
    return -1*100*np.log(df['LTP'].iloc[-1] / entry)

def backtest_top_x(date,x, n, division_factor):
    # date: the date prior to which the backtest is to be done
    # x: the number of top x instruments to be considered
    # return: the dataframe of the top x
    dists = get_all_instruments_distribution(date,n,division_factor)
    dists['mean/std'] = dists['mean'] / dists['std']
    dists.sort_values('mean',ascending=False,inplace=True)
    dists = dists.iloc[:x]
    stocks = list(dists.index)
    returns = {}
    for stock in stocks:
        returns[stock] = get_next_trading_day_returns(date,stock)
    print('Mean Returns: ',np.mean(list(returns.values())))
    return np.mean(list(returns.values()))



division_factor = 3
number_of_past_days = 30
stocks_to_trade = 3

file_path = '12to2.xlsx'
book = xw.Book(file_path)
sheetname = str(number_of_past_days)+'days'+str(stocks_to_trade)+'stocks'
try:
    book.sheets[sheetname].clear()
except:
    book.sheets.add(sheetname)
s = book.sheets[sheetname]
backtest_df = pd.DataFrame(columns=['Date','Returns'])

trading_days = get_all_trading_days()

trading_days = list(set(trading_days))
trading_days.sort()
# pr.enable()
for date in trading_days:
    if date > datetime.date(2019,2,1):
        try:
            t1 = time.time()
            returns = backtest_top_x(date,stocks_to_trade,number_of_past_days,division_factor)
            backtest_df = backtest_df.append({'Date':date,'Returns':returns},ignore_index=True)
            backtest_df['Cumulative_Future_Returns'] = backtest_df['Returns'].cumsum()*4
            print(date,returns)
            s.range('A1').value = backtest_df
            t2 = time.time()
            print('Time taken for backtest: ',t2-t1)
        except Exception as e:
            print(e)
            pass
# pr.disable()
# sortby = SortKey.CUMULATIVE
# ps = pstats.Stats(pr).sort_stats(sortby)
# ps.print_stats()

# t1 = time.time()
# print(get_all_instruments_distribution(datetime.date(2019,2,1),number_of_past_days,division_factor))
# print('Time taken: ',time.time()-t1)
# t1 = time.time()
# print(get_all_instruments_distribution(datetime.date(2019,2,1),number_of_past_days,division_factor))
# print('Time taken: ',time.time()-t1)





