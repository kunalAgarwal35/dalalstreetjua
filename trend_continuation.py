import pickle
import os
import datetime
import numpy as np
import pandas as pd
import mpl_charts as mc
oldticksdict={}
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('ticks')]

with open('April_kite.instruments', 'rb') as handle:
    instruments = (pickle.load(handle))
handle.close()

def ibat(x):
    for ins in instruments:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def loadticks(wait_min):
    global oldticksdict
    print("Loading Ticks")
    for date in dates_list:
        if date.hour == 9 and date.minute < wait_min:
            # print(date)
            try:
                with open('oldticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
def loadcustomticks(from_time,to_time):
    global oldticksdict
    print("Loading Ticks")
    for date in dates_list:
        if from_time<date.time()<to_time:
            # print(date)
            try:
                with open('oldticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
topbuy={}
topsell={}
avgbuymoney = {}
avgsellmoney = {}
ltp_open = {}
ltp_920 = {}
ltp_close = {}
tradelist = {}
def find_trades(top_x,comp_min,wait_min,trade_end_hour,trade_end_min,movemaxthres,moveminthres,bs_long,bs_short,savechart,short_historical_drop_thres,long_historical_drop_thres,historical_days):
    global oldticksdict
    topbuy = []
    topsell = []
    avgbuymoney0 = {}
    avgsellmoney0 = {}
    avgbuymoney1 = {}
    avgsellmoney1 = {}
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('oldticks')]
    oldticksdict = {}
    loadticks(wait_min)
    for item in oldticksdict.keys():
        tick = oldticksdict[item]
        for token in tick.keys():
            buy_money = tick[token]['buy_quantity'] * tick[token]['last_price'] / 100000000
            sell_money = tick[token]['sell_quantity'] * tick[token]['last_price'] / 100000000
            if token not in avgsellmoney0.keys():
                avgsellmoney0[token] = [sell_money]
            else:
                templist = avgsellmoney0[token]
                templist.append(buy_money)
                avgsellmoney0[token] = templist
            if token not in avgbuymoney0.keys():
                avgbuymoney0[token] = [buy_money]
            else:
                templist = avgbuymoney0[token]
                templist.append(sell_money)
                avgbuymoney0[token] = templist
    oldticksdict = {}
    loadticks(comp_min)
    for item in oldticksdict.keys():
        tick = oldticksdict[item]
        for token in tick.keys():
            buy_money = tick[token]['buy_quantity'] * tick[token]['last_price'] / 100000000
            sell_money = tick[token]['sell_quantity'] * tick[token]['last_price'] / 100000000
            if token not in avgsellmoney1.keys():
                avgsellmoney1[token] = [sell_money]
            else:
                templist = avgsellmoney1[token]
                templist.append(buy_money)
                avgsellmoney1[token] = templist
            if token not in avgbuymoney1.keys():
                avgbuymoney1[token] = [buy_money]
            else:
                templist = avgbuymoney1[token]
                templist.append(sell_money)
                avgbuymoney1[token] = templist

    for token in avgbuymoney0.keys():
        avgbuymoney0[token] = np.mean(avgbuymoney0[token])
    for token in avgsellmoney0.keys():
        avgsellmoney0[token] = np.mean(avgsellmoney0[token])
    for token in avgbuymoney1.keys():
        avgbuymoney1[token] = np.mean(avgbuymoney1[token])
    for token in avgsellmoney1.keys():
        avgsellmoney1[token] = np.mean(avgsellmoney1[token])

    topbuy = sorted(avgbuymoney0, key=avgbuymoney0.get, reverse=True)[:top_x]
    topsell = sorted(avgsellmoney0, key=avgsellmoney0.get, reverse=True)[:top_x]

    ltp_open = {}
    ltp_920 = {}
    tradelist = list(set([*topbuy, *topsell]))
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('ticks')]
    print('Trades ','>>>', len(tradelist))
    print(tradelist)
    for item in dates_list:
        date = item.date()
        if item.hour == 9 and item.minute < 16 and item.second < 10:
            # print(item)
            try:
                with open('ticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_open.keys():
                        ltp_open[token] = tick[token]['last_price']
            except:
                pass
        if item.hour == 9 and item.minute == wait_min and item.second < 10:
            # print(item)
            try:
                with open('ticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_920.keys():
                        ltp_920[token] = tick[token]['last_price']
            except Exception as E:
                # print(E)
                pass
    longs=[]
    shorts=[]
    for token in tradelist:
        try:
            newrow={'Token':ibat(token),'First 15':100*np.log(ltp_920[token]/ltp_open[token]),
                    'Date':date.strftime("%m%d%Y"),
                    'Avg Buy Money':avgbuymoney0[token],
                    'Avg Sell Money':avgsellmoney0[token],
                    'B/S':avgbuymoney0[token]/avgsellmoney0[token],
                    'B/S(old)':avgbuymoney1[token]/avgsellmoney1[token]}

            if (newrow['B/S'] > bs_long and newrow['First 15'] > 0) or (
                    newrow['B/S'] < bs_short and newrow['First 15'] < 0):
                if abs(newrow['First 15']) < movemaxthres and abs(newrow['First 15']) > moveminthres:
                    days30 = datetime.timedelta(days=30)
                    days1 = datetime.timedelta(days=1)
                    last_n_days=mc.historical(token,(date-days30).strftime("%Y-%m-%d"),date-days1,'day',0)
                    last_n_days=last_n_days[-1*historical_days:]
                    last_n_days=100*np.log(last_n_days['close'].to_list()[len(last_n_days)-1]/last_n_days['open'].to_list()[0])
                    if newrow['First 15'] > 0:
                        direction = 'Long'
                    else:
                        direction = 'Short'
                    if (last_n_days > long_historical_drop_thres and direction == 'Long' and newrow['B/S'] / newrow[
                        'B/S(old)'] > 1) or (
                            last_n_days < short_historical_drop_thres and direction == 'Short' and newrow['B/S'] /
                            newrow['B/S(old)'] < 1):
                        if direction=='Long':
                            longs.append(token)
                        else:
                            shorts.append(token)
                        print(token, newrow['B/S'], last_n_days, direction, newrow['B/S'] / newrow['B/S(old)'])
        except Exception as E:
            print(E)
    return longs,shorts

top_x=50
comp_min=17
wait_min=19
trade_end_hour=15
trade_end_min=15
movemaxthres=1.5
moveminthres=0.2
bs_long=1
bs_short=1
savechart=1
short_historical_drop_thres=1
long_historical_drop_thres=-1
historical_days=2

longs,shorts=find_trades(top_x,comp_min,wait_min,trade_end_hour,trade_end_min,movemaxthres,moveminthres,bs_long,bs_short,savechart,long_historical_drop_thres,short_historical_drop_thres,historical_days)
# for token in longs:
#     ts=ibat(token)
#     mc.savechart(ts+' long','ladder trades/charts/'+ts+'.png',token,'2021-04-29','2021-04-29','5minute',0)
#
# for token in shorts:
#     ts=ibat(token)
#     mc.savechart(ts+' short','ladder trades/charts/'+ts+'.png',token,'2021-04-29','2021-04-29','5minute',0)