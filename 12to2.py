import pandas as pd
import numpy as np
import os
import xlwings as xw
import time
import datetime

def fname_to_instrument(fname):
    return fname.split('-')[0]
def fname_to_date(fname):
    return datetime.datetime.strptime(fname.split('_')[2], '%d%m%Y').date()
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
def get_trading_days(month_foldername):
    # get the trading days of the month
    # month_year: the month and year
    # return: the trading days of the month
    # example: get_trading_days('Jan_2018')
    month_year = month_foldername.upper()
    data_loc = 'E:/tickdata/'
    available_days = [fname_to_date(x) for x in os.listdir(data_loc + month_foldername)]
    return available_days
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

file_path = '12to2.xlsx'
book = xw.Book(file_path)
s = book.sheets['Sheet1']








