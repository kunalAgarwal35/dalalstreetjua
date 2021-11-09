import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import datetime
import numpy as np
from gspread_pandas import Spread
import xlwings as xw
import pickle
import time
file_path = 'live.xlsx'
book = xw.Book(file_path)
s = book.sheets['banknifty']
s2 = book.sheets['nifty_credit']
# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
file = client.open("malamaal weekly")
ws = file.worksheet("banknifty_all")
ws2 = file.worksheet("nifty_credit")
spreadsheet_key = file.id
wsname = 'banknifty_all'
wsname2 = 'nifty_credit'
ws.clear()
ws2.clear()
open_pos = s.range('F1').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
pnl = s.range('F6').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
threses = s.range('I6').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
ledger = s.range('S1').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
payoff = s.range('J17').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
ecurve = payoff = s.range('AF1').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
x= Spread(spreadsheet_key, wsname,creds=creds,client=client)
x2= Spread(spreadsheet_key, wsname2,creds=creds,client=client)

while s.range('K1'):
    evs = s.range('A1').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
    x.df_to_sheet(df=evs,index=0,headers=0,sheet=wsname,replace=False,start='A1')

    open_pos = s.range('F1').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
    x.df_to_sheet(df=open_pos, index=0, headers=0, sheet=wsname, replace=False, start='F1')

    pnl = s.range('F6').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
    x.df_to_sheet(df=pnl, index=0, headers=0, sheet=wsname, replace=False, start='F6')

    threses = s.range('I6').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
    x.df_to_sheet(df=threses, index=0, headers=0, sheet=wsname, replace=False, start='I6')

    payoff = s.range('J17').options(pd.DataFrame,expand = 'table',header = 0,index=0).value
    x.df_to_sheet(df=payoff, index=0, headers=0, sheet=wsname, replace=False, start='J17')

    ledger = s.range('S1').options(pd.DataFrame, expand='table', header=0, index=0).value
    x.df_to_sheet(df=ledger, index=0, headers=0, sheet=wsname, replace=False, start='L1')

    ecurve = s.range('AF1').options(pd.DataFrame, expand='table', header=0, index=0).value
    x.df_to_sheet(df=ecurve, index=0, headers=0, sheet=wsname, replace=False, start='AF1')


    evs = s2.range('A1').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=evs, index=0, headers=0, sheet=wsname2, replace=False, start='A1')

    open_pos = s2.range('F1').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=open_pos, index=0, headers=0, sheet=wsname2, replace=False, start='F1')

    pnl = s2.range('F6').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=pnl, index=0, headers=0, sheet=wsname2, replace=False, start='F6')

    threses = s2.range('I6').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=threses, index=0, headers=0, sheet=wsname2, replace=False, start='I6')

    payoff = s2.range('J17').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=payoff, index=0, headers=0, sheet=wsname2, replace=False, start='J17')

    ledger = s2.range('S1').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=ledger, index=0, headers=0, sheet=wsname2, replace=False, start='L1')

    ecurve = s2.range('AF1').options(pd.DataFrame, expand='table', header=0, index=0).value
    x2.df_to_sheet(df=ecurve, index=0, headers=0, sheet=wsname2, replace=False, start='AF1')
    print('Sleeping for 5 seconds')
    time.sleep(5)



