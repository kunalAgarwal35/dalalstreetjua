import discord
import os
import pickle
import numpy as np
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import datetime
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import pandas as pd
import time
from furl import furl
import configparser
import _thread as thread
from discord_webhook import DiscordWebhook
import matplotlib.pyplot as plt


class ThreadSafeDict(dict) :
    def __init__(self, * p_arg, ** n_arg) :
        dict.__init__(self, * p_arg, ** n_arg)
        self._lock = threading.Lock()

    def __enter__(self) :
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback) :
        self._lock.release()

config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['DEFAULT']['api_key']
api_secret = config['DEFAULT']['api_secret']
username = config['DEFAULT']['username']
password = config['DEFAULT']['password']
pin = config['DEFAULT']['pin']
now = datetime.datetime.now()
last_tick = {}
monitoring = []
firsttick = 0
discordtoken = config['DEFAULT']['discordtoken']
wbhk=config['DEFAULT']['wbhk']
param={}
trespassers={}
lastbidalert={}
lastaskalert={}
ts={}
yesterday_oi={}
client = discord.Client()
# fuction to wait for elements on page load for selenium
def getCssElement(driver, cssSelector):
    return WebDriverWait(driver, 100).until(EC.presence_of_element_located((By.CSS_SELECTOR, cssSelector)))


# function to login to zerodha using selenium
def autologin():
    kite = KiteConnect(api_key=api_key)
    service = webdriver.chrome.service.Service('./chromedriver')
    service.start()
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options = options.to_capabilities()
    driver = webdriver.Remote(service.service_url, options)
    driver.get(kite.login_url())

    passwordField = getCssElement(driver, "input[placeholder=Password]")
    passwordField.send_keys(password)

    userNameField = getCssElement(driver, "input[placeholder='User ID (eg: AB0001)']")
    userNameField.send_keys(username)

    loginButton = getCssElement(driver, "button[type=submit]")
    loginButton.click()

    WebDriverWait(driver, 100).until(EC.presence_of_element_located((By.CLASS_NAME, 'twofa-value')))
    pinField = driver.find_element_by_class_name('twofa-value').find_element_by_xpath(".//input[1]")
    pinField.send_keys(pin)

    loginButton = getCssElement(driver, "button[type=submit]")
    loginButton.click()

    while True:
        try:
            request_token = furl(driver.current_url).args['request_token'].strip()
            break
        except:
            time.sleep(1)
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    with open('access_token.txt', 'w') as file:
        file.write(data["access_token"])
    driver.quit()


autologin()

# retriving access token from saved file
access_token_zerodha = open("access_token.txt", 'r').read()

# #creating kite connect object
kite = KiteConnect(api_key=api_key)

# # setting access token ti kite connect object
kite.set_access_token(access_token_zerodha)

# kite ticker object to recieve data from zerodha
kws = KiteTicker(api_key, access_token_zerodha)

allins = kite.instruments()
monitoring = allins
curr_ins={}
for ins in allins:
    curr_ins[ins['instrument_token']]=ins
def pingdiscord(txt):
    pdiscord = int(config['PARAMS']['pingdiscord'])
    if pdiscord:
        webhook = DiscordWebhook(url=wbhk, content=txt)
        response = webhook.execute()

def historical(token,fdate,tdate,interval,o_i):
    if type(token)==int and token in curr_ins.keys():
        ohlc=pd.DataFrame(kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval=interval,oi=o_i))
        return ohlc
    elif type(token)==str:
        for ins in curr_ins.keys():
            if curr_ins[ins]['tradingsymbol']==token:
                token=curr_ins[ins]['instrument_token']
                ohlc = pd.DataFrame(
                    kite.historical_data(instrument_token=token, from_date=fdate, to_date=tdate, interval=interval,
                                            oi=o_i))
                return ohlc
        for ins in old_ins.keys():
            if old_ins[ins]['tradingsymbol']==token:
                token=old_ins[ins]['instrument_token']
                for new_key in curr_ins.keys():
                    if curr_ins[new_key]['tradingsymbol']==old_ins[key]['tradingsymbol']:
                        token=curr_ins[new_key]['instrument_token']
                        ohlc = pd.DataFrame(
                            kite.historical_data(instrument_token=token, from_date=fdate, to_date=tdate, interval=interval,
                                                    oi=o_i))
                        return ohlc
    else:
        for key in old_ins.keys():
            if old_ins[key]['instrument_token']==token:
                for new_key in curr_ins.keys():
                    if curr_ins[new_key]['tradingsymbol']==old_ins[key]['name']:
                        token=curr_ins[new_key]['instrument_token']
                        ohlc = pd.DataFrame(
                            kite.historical_data(instrument_token=token, from_date=fdate, to_date=tdate,
                                                    interval=interval,
                                                    oi=o_i))
                        return ohlc

# def average_volumes(n):
#     tokens=nifty250fut_tokens()
#     dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('oldticks')]
#     eod_vol={}
#     for date in dates_list:
#         if date.date() not in eod_vol.keys():
#             if date.hour == 15 and date.minute==15:
#                 eod_vol[date.date()]={}
#                 for token in tokens:
#
#         volav
def monetary_interest_2():
    # t1=time.time()
    config.read('config.ini')
    n = int(config['PARAMS']['relative_volume_interval'])
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('ticks')]
    if len(dates_list)>125:
        tick80 = dates_list[len(dates_list)-80]
        tick30 = dates_list[len(dates_list)-30]
        with open('ticks/' + tick80.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
            tick80 = (pickle.load(handle))
        handle.close()
        with open('ticks/' + tick30.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
            tick20 = (pickle.load(handle))
        handle.close()

    midf = pd.DataFrame(columns=['script', 'buy_orders', 'sell_orders', 'b/s','b/s 80','b/s 30','oi change','% change'])
    # print(len(last_tick.keys()))
    for token in last_tick.keys():
        try:
            newmidfrow={'script':ts[token],'buy_orders':last_tick[token]['buy_quantity']*last_tick[token]['last_price']/1000000,
                        'sell_orders':last_tick[token]['sell_quantity']*last_tick[token]['last_price']/1000000,
                        'b/s': last_tick[token]['buy_quantity'] / last_tick[token]['sell_quantity'],
                        'oi change':100*np.log(last_tick[token]['oi']/yesterday_oi[token]),
                        '% change':last_tick[token]['change']}
            if len(dates_list)>125 and token in tick80 and token in tick20:
                newmidfrow['b/s 80']=newmidfrow['b/s']-(tick80[token]['buy_quantity'] / tick80[token]['sell_quantity'])
                newmidfrow['b/s 30'] = newmidfrow['b/s']-(tick20[token]['buy_quantity'] / tick20[token]['sell_quantity'])
            midf=midf.append(newmidfrow, ignore_index=True)
        except Exception as E:
            print('Error in monetory_interest\n',E)
            continue

    # print(time.time() - t1)
    midf.to_csv('monetary_interst.csv', index=False)
    # print(time.time()-t1)
def monetary_interest():
    # t1=time.time()
    config.read('config.ini')
    n = int(config['PARAMS']['relative_volume_interval'])

    midf = pd.DataFrame(columns=['script', 'buy_orders', 'sell_orders', 'b/s','oi change','% change'])
    # print(len(last_tick.keys()))
    for token in last_tick.keys():
        try:
            newmidfrow={'script':ts[token],'buy_orders':last_tick[token]['buy_quantity']*last_tick[token]['last_price']/1000000,
                        'sell_orders':last_tick[token]['sell_quantity']*last_tick[token]['last_price']/1000000,
                        'b/s': last_tick[token]['buy_quantity'] / last_tick[token]['sell_quantity'],
                        'oi change':100*np.log(last_tick[token]['oi']/yesterday_oi[token]),
                        '% change':last_tick[token]['change']}
            midf=midf.append(newmidfrow, ignore_index=True)
        except Exception as E:
            print('Error in monetory_interest\n',E)
            continue

    # print(time.time() - t1)
    midf.to_csv('monetary_interst.csv', index=False)
    # print(time.time()-t1)
def monitor_orders():
    config = configparser.ConfigParser()
    config.read('config.ini')
    orderchange_alert_thres=float(config['PARAMS']['orderchange_alert_thres'])
    n=int(config['PARAMS']['monitor_orders_interval'])
    dates_list = os.listdir('ticks')
    if len(dates_list)>n+1:
        date=dates_list[len(dates_list)-n-1]
        with open('ticks/' + date, 'rb') as handle:
            prev_tick = (pickle.load(handle))
        handle.close()
        buyorderschange={}
        sellorderschange={}
        try:
            for token in last_tick:
                if token in prev_tick:
                    buyorderschange[token]=100*np.log(last_tick[token]['buy_quantity']/prev_tick[token]['buy_quantity'])
                    sellorderschange[token] = 100 * np.log(last_tick[token]['sell_quantity'] / prev_tick[token]['sell_quantity'])
                    if abs(buyorderschange[token])>orderchange_alert_thres:
                        url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[token] + '/' + str(token)
                        txt='Buy Orders Changed in '+ts[token]+' by'+buyorderschange[token]+'\n'+url
                        print(txt)
                        pingdiscord(txt)
                    if abs(sellorderschange[token])>orderchange_alert_thres:
                        url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[token] + '/' + str(token)
                        txt='sell Orders Changed in '+ts[token]+' by'+sellorderschange[token]+'\n'+url
                        print(txt)
                        pingdiscord(txt)
        except:
            pass


def loadticks(wait_min,dates_list):
    global oldticksdict
    # print("Loading Ticks")
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
def loadcustomticks(comp_min,wait_min,dates_list):
    global oldticksdict
    # print("Loading Ticks")
    for date in dates_list:
        if comp_min < date < wait_min:
            # print(date)
            try:
                with open('ticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
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
    loadticks(wait_min,dates_list)
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
    loadticks(comp_min,dates_list)
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
    # print('Trades ','>>>', len(tradelist))
    # print(tradelist)
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
                    last_n_days=historical(token,(date-days30).strftime("%Y-%m-%d"),date-days1,'day',0)
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
                        # print(token, newrow['B/S'], last_n_days, direction, newrow['B/S'] / newrow['B/S(old)'])
        except Exception as E:
            # print(E)
            pass
    return longs,shorts
def find_trades_anytime(top_x,delta,comp_min,wait_min,trade_end_hour,trade_end_min,movemaxthres,moveminthres,bs_long,bs_short,savechart,short_historical_drop_thres,long_historical_drop_thres,historical_days):
    topbuy = []
    topsell = []
    avgbuymoney0 = {}
    avgsellmoney0 = {}
    avgbuymoney1 = {}
    avgsellmoney1 = {}
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('ticks')]
    oldticksdict = {}
    min2=datetime.timedelta(minutes=delta)
    for date in dates_list:
        if comp_min < date < wait_min:
            # print(date)
            try:
                with open('ticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
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
    for date in dates_list:
        if comp_min-min2 < date < comp_min:
            # print(date)
            try:
                with open('ticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
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
    # print('Trades ','>>>', len(tradelist))
    # print(tradelist)
    for item in dates_list:
        date = item.date()
        if item.hour == comp_min.hour and item.minute == comp_min.minute:
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
        if item.hour == wait_min.hour and item.minute == wait_min.minute:
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
                    # last_n_days=historical(token,(date-days30).strftime("%Y-%m-%d"),date-days1,'day',0)
                    # last_n_days=last_n_days[-1*historical_days:]
                    # last_n_days=100*np.log(last_n_days['close'].to_list()[len(last_n_days)-1]/last_n_days['open'].to_list()[0])
                    if newrow['First 15'] > 0:
                        direction = 'Long'
                    else:
                        direction = 'Short'
                    if (direction == 'Long' and newrow['B/S'] / newrow['B/S(old)'] > 1) or (direction == 'Short' and newrow['B/S']/newrow['B/S(old)'] < 1):
                        if direction=='Long':
                            longs.append(token)
                        else:
                            shorts.append(token)
                        # print(token, newrow['B/S'], last_n_days, direction, newrow['B/S'] / newrow['B/S(old)'])
        except Exception as E:
            # print(E)
            pass
    return longs,shorts
def trade_tct_anytime(amount):
    now = datetime.datetime.now()
    config = configparser.ConfigParser()
    config.read('config.ini')
    top_x = int(config['TRADE']['top_x'])
    delta = int(config['TRADE']['delta'])
    trade_end_hour = int(config['TRADE']['trade_end_hour'])
    trade_end_min = int(config['TRADE']['trade_end_min'])
    movemaxthres = float(config['TRADE']['movemaxthres'])
    moveminthres = float(config['TRADE']['moveminthres'])
    bs_long = float(config['TRADE']['bs_long'])
    bs_short = float(config['TRADE']['bs_short'])
    savechart = int(config['TRADE']['savechart'])
    short_historical_drop_thres = float(config['TRADE']['short_historical_drop_thres'])
    long_historical_drop_thres = float(config['TRADE']['long_historical_drop_thres'])
    historical_days = int(config['TRADE']['historical_days'])
    wait_min=now
    mindelta = datetime.timedelta(minutes=delta)
    comp_min=now-mindelta
    longs, shorts = find_trades_anytime(top_x, delta, comp_min, wait_min, trade_end_hour, trade_end_min, movemaxthres, moveminthres,
                                bs_long, bs_short, savechart, long_historical_drop_thres, short_historical_drop_thres,
                                historical_days)
    print(longs)
    print(shorts)
    qty={}
    tottrades = len(longs) + len(shorts)
    if tottrades > 0:
        apt = amount / tottrades
        while len(qty.keys()) < tottrades:
            for token in last_tick.keys():
                if token in longs:
                    qty[token] = apt / last_tick[token]['last_price']
                if token in shorts:
                    qty[token] = -1 * apt / last_tick[token]['last_price']
    else:
        print('No Trades')
    doutput = ''
    oldlongs=[]
    oldshorts=[]
    if 'longs.pickle' in os.listdir():
        open_file = open('longs.pickle', "rb")
        oldlongs = pickle.load(open_file)
        open_file.close()
    if 'shorts.pickle' in os.listdir():
        open_file = open('shorts.pickle', "rb")
        oldshorts = pickle.load(open_file)
        open_file.close()

    open_file = open('longs.pickle', "wb")
    pickle.dump(longs, open_file)
    open_file.close()

    open_file = open('shorts.pickle', "wb")
    pickle.dump(shorts, open_file)
    open_file.close()

    for short in shorts:
        if short not in oldshorts:
            url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[short] + '/' + str(short)
            doutput=doutput+'New Short: '+ts[short]+' ['+str(qty[short])+']'+'\n'+url+'\n'
    for short in oldshorts:
        if short not in shorts:
            url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[short] + '/' + str(short)
            doutput = doutput + 'Close Short: ' +ts[short]+'\n'+url+'\n'
    doutput=doutput+'----------------------- \n'
    for long in longs:
        if long not in oldlongs:
            url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[long] + '/' + str(long)
            doutput=doutput+'New long: '+ts[long]+' ['+str(qty[long])+']' +'\n'+url+'\n'
    for long in oldlongs:
        if long not in longs:
            url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[long] + '/' + str(long)
            doutput = doutput+'Close long: ' + ts[long]+ '\n'+url+'\n'
    if len(doutput)>26:
        pingdiscord(doutput)
    tottrades=len(longs)+len(shorts)

def vapgraph(symbol):
    token=list(ts.keys())[list(ts.values()).index(symbol)]
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('ticks')]

    volume=0
    vapdict={}
    for date in dates_list:
        with open('ticks/'+date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
            tick = (pickle.load(handle))
            tick=tick[token]
        handle.close()
        price=tick['last_price']
        voltraded=tick['volume']-volume
        volume=tick['volume']
        if price not in vapdict:
            vapdict[price]=voltraded
        else:
            vapdict[price]=vapdict[price]+voltraded

    keys = vapdict.keys()
    values = vapdict.values()
    plt.bar(keys,values)
    return keys,values


def trade_tct(amount):
    now = datetime.datetime.now()
    config = configparser.ConfigParser()
    config.read('config.ini')
    top_x = int(config['TRADE']['top_x'])
    comp_min = int(config['TRADE']['comp_min'])
    wait_min = int(config['TRADE']['wait_min'])
    trade_end_hour = int(config['TRADE']['trade_end_hour'])
    trade_end_min = int(config['TRADE']['trade_end_min'])
    movemaxthres = float(config['TRADE']['movemaxthres'])
    moveminthres = float(config['TRADE']['moveminthres'])
    bs_long = float(config['TRADE']['bs_long'])
    bs_short = float(config['TRADE']['bs_short'])
    savechart = int(config['TRADE']['savechart'])
    short_historical_drop_thres = float(config['TRADE']['short_historical_drop_thres'])
    long_historical_drop_thres = float(config['TRADE']['long_historical_drop_thres'])
    historical_days = int(config['TRADE']['historical_days'])
    longs, shorts = find_trades(top_x, comp_min, wait_min, trade_end_hour, trade_end_min, movemaxthres, moveminthres,
                                bs_long, bs_short, savechart, long_historical_drop_thres, short_historical_drop_thres,
                                historical_days)
    print(longs)
    print(shorts)
    qty={}
    tottrades=len(longs)+len(shorts)
    if tottrades>0:
        apt=amount/tottrades
        while len(qty.keys())<tottrades:
            for token in last_tick.keys():
                if token in longs:
                    qty[token]=apt/last_tick[token]['last_price']
                    tsym = ts[token]
                    url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + tsym + '/' + str(token)
                    pingdiscord(url+'\n'+str(qty[token]))
                if token in shorts:
                    qty[token] = -1*apt / last_tick[token]['last_price']
                    tsym = ts[token]
                    url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + tsym + '/' + str(token)
                    pingdiscord(url + '\n' + str(qty[token]))
    else:
        print('No Trades')

    print(qty)
def ib153t():
    #updates dictionary for 153 monitored futures
    for token in nifty250fut_tokens():
        for ins in allins:
            if ins['instrument_token']==token:
                ts[token]=ins['tradingsymbol']
def update_yesterday_oi():
    date=list(reversed(os.listdir('oldticks')))[1]
    with open('oldticks/' + date, 'rb') as handle:
        tick = (pickle.load(handle))
    handle.close()
    for token in tick:
        yesterday_oi[token]=tick[token]['oi']
def ibt(x):
    for ins in monitoring:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def ibat(x):
    for ins in allins:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def dumptick():
    pickle.dump(last_tick, open('ticks/' + datetime.datetime.now().strftime("%m%d%Y-%H%M%S"), "wb"))
def on_ticks(ws, ticks):
    # print('On Ticks Triggered')
    for x in ticks:
        last_tick[x['instrument_token']] = x
    thread.start_new_thread(dumptick, ())
    # print('Sending tick length '+str(len(last_tick.keys())))
    thread.start_new_thread(monetary_interest_2,())
    now=datetime.datetime.now()
    monitor_orders()
    if now.minute%2==0 and now.second<2:
        trade_tct_anytime(500000)

    # frontrun()
    # find_trespassers()
def on_connect(ws, response):
    print("Connected")
def on_close(ws, code, reason):
    pass
def startbot():
    client.run(discordtoken)
def changesubscriptions(newtokenslist):
    print("Should Receive ", len(newtokenslist))
    oldsub = list(last_tick.keys())
    kws.close()
    kws.unsubscribe(oldsub)
    kws.subscribe(newtokenslist)
    kws.set_mode(kws.MODE_FULL, newtokenslist)
    kws.connect(threaded=1)
    time.sleep(1)
def nifty250fut_tokens():
    csv=pd.read_csv("nifty250.csv")
    symbols=list(csv['Symbol'])
    tokens=[]
    instruments=[]
    types = ['FUT']
    expfut = 100
    instru=kite.instruments()
    now = datetime.datetime.now()
    for ins in instru:
        if ins['instrument_type'] == 'FUT' and ins['name'] in symbols:
            days = (ins['expiry'] - now.date()).days
            if days < expfut:
                # print(ins['tradingsymbol'])
                expfut = days
    for ins in instru:
        if ins['name'] in symbols and ins['instrument_type']=='FUT':
            daystoexpiry = (ins['expiry'] - now.date()).days
            # print(daystoexpiry)
            if daystoexpiry == expfut:
                # print(ins['tradingsymbol'])
                tokens.append(ins['instrument_token'])
                instruments.append(ins)
    return tokens


with open('askthres.pickle', 'rb') as handle:
    askstres = pickle.load(handle)
handle.close()
with open('bidthres.pickle', 'rb') as handle:
    bidstres = pickle.load(handle)
handle.close()

def frontrun():
    global lastbidalert, lastaskalert
    t1=time.time()
    tick=last_tick
    for token in tick.keys():
        if 'depth' in tick[token].keys() and token in bidstres.keys():
            itr = tick[token]['depth']
            for i in range(0,5):
                if itr['buy'][i]['quantity'] > bidstres[token]:
                    if token not in lastbidalert:
                        lastbidalert[token]=time.time()
                        pingdis=1
                    else:
                        if lastbidalert[token]-time.time()>120:
                            pingdis=1
                        else:
                            pingdis=0

                    tsym = ibt(token)
                    url='https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/'+tsym+'/'+str(token)
                    pdiscord = int(config['PARAMS']['pingdiscord'])
                    if pingdis and pdiscord:
                        pingdiscord(tsym+ '---Bids---'+'\n'+url+'\n'+str(itr['buy']))
                    print(tsym+ '---Bids---'+'\n'+url+'\n'+str(itr['buy']))
                    # print("Frontrunning time: ",t1-time.time())
                    break
                if itr['sell'][i]['quantity'] > askstres[token]:
                    if token not in lastaskalert:
                        lastbidalert[token] = time.time()
                        pingdis = 1
                    else:
                        if lastaskalert[token] - time.time() > 120:
                            pingdis = 1
                        else:
                            pingdis = 0

                    tsym = ibt(token)
                    url='https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/'+tsym+'/'+str(token)
                    pdiscord = int(config['PARAMS']['pingdiscord'])
                    if pingdis and pdiscord:
                        pingdiscord(tsym+' ---Asks---'+'\n'+url+'\n'+str(itr['sell']))
                    print(tsym+' ---Asks---'+'\n'+url+'\n'+str(itr['sell']))
                    # print("Frontrunning time: ",t1-time.time())
                    break
def loadparams():
    param['gaptres']=config['PARAMS']['gaptres']
def updateparameters(txt):
    if '$gaptres' in txt:
        p=float(txt.replace('$gaptres',''))
        config['PARAMS']['gaptres']=str(p)
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        configfile.close()
    if '$pingdiscord' in txt:
        p=int(txt.replace('$pingdiscord',''))
        config['PARAMS']['pingdiscord']=str(p)
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        configfile.close()
    if '$oi_change_tres' in txt:
        p=float(txt.replace('$oi_change_tres',''))
        config['PARAMS']['oi_change_tres']=str(p)
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        configfile.close()

@client.event
async def on_ready():
    print('Logged in as {0.user}'.format(client))
    kws.close()
thread.start_new_thread(startbot, ())
def cleantrespassers():
    os.remove('trespassers.pickle')
def find_trespassers():
    try:
        with open('trespassers.pickle', 'rb') as handle:
            trespassers = pickle.load(handle)
        handle.close()
    except:
        trespassers={}
    ticklist=os.listdir('ticks')
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in ticklist]
    for date in reversed(dates_list):
        timediff=(datetime.datetime.now()-date).seconds
        if timediff>300:
            break
    with open('ticks/'+date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
        oldtick = pickle.load(handle)
    handle.close()

    for x in last_tick.keys():
        if x in oldtick:
            diff=100*np.log(last_tick[x]['last_price']/oldtick[x]['last_price'])
            if abs(diff)>float(config['PARAMS']['gaptres']):
                tsym=ts[x]
                url='https://kite.zerodha.com/chart/web/tvc/NFO-FUT/'+tsym+'/'+str(x)
                if tsym not in trespassers.keys():
                    trespassers[tsym]=last_tick[x]['last_price']
                    print(tsym + ': ' + str(last_tick[x]['last_price']))
                    pingdiscord(tsym+': ['+str(oldtick[x]['last_price'])+' to '+str(last_tick[x]['last_price'])+']\n'+url)
                else:
                    diff = 100 * np.log(last_tick[x]['last_price']/trespassers[tsym])
                    if abs(diff) > float(config['PARAMS']['gaptres']):
                        trespassers[tsym] = last_tick[x]['last_price']
                        print(tsym+': '+str(last_tick[x]['last_price']))
                        pingdiscord('Watch again: '+tsym+': ['+str(trespassers[tsym])+' to '+str(last_tick[x]['last_price'])+']\n'+url)
        if x in oldtick:
            diff=100*np.log(last_tick[x]['oi']/oldtick[x]['oi'])
            if abs(diff)>float(config['PARAMS']['oi_change_tres']) and (str(x)+' oiping' not in trespassers.keys() or time.time()-trespassers[str(x)+' oiping']>30):
                trespassers[str(x) + ' oiping']=time.time()
                url = 'https://kite.zerodha.com/chart/web/tvc/NFO-FUT/' + ts[x] + '/' + str(x)
                print(ts[x] + ' OI Changed by: ' + str(diff))
                pingdiscord(ts[x] + ' OI Changed by: ' + str(diff))

    with open('trespassers.pickle', 'wb') as handle:
        pickle.dump(trespassers, handle, protocol=pickle.HIGHEST_PROTOCOL)
    handle.close()
    return len(trespassers)



@client.event
async def on_message(message):
    if message.author == client.user:
        return
    user=str(message.author)
    authenticated_users=['monster#3020']
    if user not in authenticated_users:
        return
    else:
        txt = message.content
        if txt=='*start250':
            print("Starting Ticker")
            try:
                changesubscriptions(nifty250fut_tokens())
            except:
                try:
                    changesubscriptions(nifty250fut_tokens())
                except:
                    print("Failed")
            loadparams()
        if txt=='*stopticker':
            print("Stopping Ticker")
            kws.close()
        if message.content.startswith('$'):
            updateparameters(txt)
        if txt.lower()=='clean trespassers':
            cleantrespassers()
        if 'tradetct' in txt:
            amt=int(txt.replace('tradetct',''))
            trade_tct(amt)
        if 'laddernow' in txt:
            amt=int(txt.replace('laddernow',''))
            trade_tct_anytime(amt)
        if 'vapgraph' in txt:
            symbol=txt.replace('vapgraph','').replace(' ','')
            keys,values=vapgraph(symbol)
            plt.bar(keys,values)


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

ib153t()
update_yesterday_oi()

kws.connect(threaded=1)
changesubscriptions(nifty250fut_tokens())

