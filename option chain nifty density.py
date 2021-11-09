import time
import pickle
import os
import pandas as pd
import numpy as np
import nsetools as nse
import nsepython as nsep
import datetime
import xlwings as xw
file_path='temp.xlsx'
sheet1=xw.Book(file_path).sheets['Sheet1']

next_expiry = datetime.date(2021,6,24)
def get_option_chain(next_expiry):
    optch=nsep.option_chain('NIFTY')
    optch=optch['records']['data']
    fil_optch=[]
    for item in optch:
        if datetime.datetime.strptime(item['expiryDate'],"%d-%b-%Y").date()==next_expiry and len(item.keys())>3:
            fil_optch.append(item)
    calls=pd.DataFrame()
    puts=pd.DataFrame()
    for item in fil_optch:
        calls=calls.append(item['CE'],ignore_index=True)
        puts=puts.append(item['PE'],ignore_index=True)

    useful_columns=['askPrice','askQty','bidprice','change','changeinOpenInterest','totalTradedVolume','impliedVolatility','lastPrice','openInterest','strikePrice']
    calls=calls[useful_columns]
    puts=puts[useful_columns]
    # sheet1.range('A1').value=calls
    return calls,puts


def get_nifty_distribution(min_spot,max_spot,calls,puts):
    next_expiry = datetime.date(2021, 6, 24)
    lot_size = 75
    density=pd.DataFrame()
    density['price']=np.arange(min_spot,max_spot,5)
    density['carlo']=0
    puts = puts.sort_values(by=['strikePrice'], ascending=False).reset_index(drop=True)
    #Simulating Credit Spreads
    for sell_loc in range(0,len(puts)-2):
        for buy_loc in range(sell_loc+1,len(puts)-1):
            sell_price = puts.loc[sell_loc]['bidprice']
            buy_price = puts.loc[buy_loc]['askPrice']
            credit = sell_price - buy_price
            width = puts.loc[sell_loc]['strikePrice']-puts.loc[buy_loc]['strikePrice']
            be = puts.loc[sell_loc]['strikePrice'] - credit
            pop = 1-(credit/width)
            oi = min(puts.loc[sell_loc]['openInterest'],puts.loc[buy_loc]['openInterest'])
            density.loc[density.price > be, 'carlo'] += pop*oi
            density.loc[density.price < be, 'carlo'] += (1-pop)*oi

            # density.plot(x='price',y='carlo')
        # sheet1.range('A1').value=density
    for sell_loc in range(0,len(calls)-2):
        for buy_loc in range(sell_loc+1,len(calls)-1):
            sell_price = calls.loc[sell_loc]['bidprice']
            buy_price = calls.loc[buy_loc]['askPrice']
            credit = sell_price - buy_price
            width = calls.loc[buy_loc]['strikePrice']-calls.loc[sell_loc]['strikePrice']
            be = calls.loc[sell_loc]['strikePrice'] + credit
            pop = 1-(credit/width)
            oi = min(calls.loc[sell_loc]['openInterest'],calls.loc[buy_loc]['openInterest'])
            density.loc[density.price < be, 'carlo'] += pop*oi
            density.loc[density.price > be, 'carlo'] += (1-pop)*oi
            # density.plot(x='price',y='carlo')
        # sheet1.range('A1').value=density

    #Simulating Debit Spreads
    puts = puts.sort_values(by=['strikePrice'], ascending=True).reset_index(drop=True)
    for sell_loc in range(0,len(puts)-2):
        for buy_loc in range(sell_loc+1,len(puts)-1):
            sell_price = puts.loc[sell_loc]['bidprice']
            buy_price = puts.loc[buy_loc]['askPrice']
            credit = sell_price - buy_price
            debit = -1 * credit
            width = puts.loc[sell_loc]['strikePrice']-puts.loc[buy_loc]['strikePrice']
            be = puts.loc[buy_loc]['strikePrice'] + credit
            max_profit = abs(width)-abs(credit)
            pop = 1-(max_profit/abs(width))
            oi = min(puts.loc[sell_loc]['openInterest'],puts.loc[buy_loc]['openInterest'])
            density.loc[density.price < be, 'carlo'] += pop*oi
            density.loc[density.price > be, 'carlo'] += (1-pop)*oi
            # density.plot(x='price',y='carlo')
        # sheet1.range('A1').value=density
    calls = calls.sort_values(by=['strikePrice'], ascending=False).reset_index(drop=True)
    for sell_loc in range(0,len(calls)-2):
        for buy_loc in range(sell_loc+1,len(calls)-1):
            sell_price = calls.loc[sell_loc]['bidprice']
            buy_price = calls.loc[buy_loc]['askPrice']
            credit = sell_price - buy_price
            width = calls.loc[buy_loc]['strikePrice']-calls.loc[sell_loc]['strikePrice']
            be = calls.loc[buy_loc]['strikePrice'] + abs(credit)
            max_profit = abs(width)-abs(credit)
            pop = 1-(max_profit/abs(width))
            oi = min(calls.loc[sell_loc]['openInterest'],calls.loc[buy_loc]['openInterest'])
            density.loc[density.price > be, 'carlo'] += pop*oi
            density.loc[density.price < be, 'carlo'] += (1-pop)*oi
            # density.plot(x='price',y='carlo')
        # sheet1.range('A1').value=density


    density['carlo'] = density['carlo'] / (density['carlo'].sum())
    sheet1.range('A1').value = density
    return density

def options_ev(min_spot,max_spot):
    min_spot = 15000
    max_spot = 16400
    calls, puts = get_option_chain(next_expiry)
    calls = calls[(calls['strikePrice'] >= min_spot) & (calls['strikePrice'] <= max_spot)].reset_index(drop=True)
    puts = puts[(puts['strikePrice'] >= min_spot) & (puts['strikePrice'] <= max_spot)].reset_index(drop=True)
    density=get_nifty_distribution(min_spot,max_spot,calls,puts)
    call_evs = pd.DataFrame()
    put_evs = pd.DataFrame()
    call_evs=density.copy()
    put_evs=density.copy()
    call_ev_dict = {}
    put_ev_dict = {}
    for i in range(0,len(calls)-1):
        strike=calls['strikePrice'][i]
        key=strike
        premium = calls['askPrice'][i]
        call_evs[key] = call_evs['price'] - strike
        call_evs[key].values[call_evs[key]<0]=0
        call_evs[key] = call_evs[key] - premium
        call_evs[key] = call_evs[key] * call_evs['carlo']
        key=-1*strike
        premium = calls['bidprice'][i]
        call_evs[key] = call_evs['price'] - strike
        call_evs[key].values[call_evs[key] < 0] = 0
        call_evs[key] = call_evs[key] - premium
        call_evs[key] = -call_evs[key]
        call_evs[key] = call_evs[key]*call_evs['carlo']
    for i in range(0,len(puts)-1):
        strike=puts['strikePrice'][i]
        key=strike
        premium = puts['askPrice'][i]
        put_evs[key] = strike - put_evs['price']
        put_evs[key].values[put_evs[key] < 0] = 0
        put_evs[key] = put_evs[key] - premium
        put_evs[key] = put_evs[key] * put_evs['carlo']
        key=-1*strike
        premium = puts['bidprice'][i]
        put_evs[key] = strike - put_evs['price']
        put_evs[key].values[put_evs[key] < 0] = 0
        put_evs[key] = put_evs[key] - premium
        put_evs[key] = - put_evs[key]
        put_evs[key] = put_evs[key]*put_evs['carlo']
    for column in call_evs:
        _=['price','carlo']
        if column not in _:
            call_ev_dict[column]=call_evs[column].mean()
    for column in put_evs:
        _=['price','carlo']
        if column not in _:
            put_ev_dict[column]=put_evs[column].mean()
    xw.Book(file_path).sheets['Sheet2'].range('A1').value=call_ev_dict
    xw.Book(file_path).sheets['Sheet2'].range('C1').value = put_ev_dict

    xw.Book(file_path).sheets['Sheet3'].range('A1').value = call_evs
    xw.Book(file_path).sheets['Sheet4'].range('A1').value = put_evs

