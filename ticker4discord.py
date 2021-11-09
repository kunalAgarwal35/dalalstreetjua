# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 12:30:44 2021

@author: Kunal
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 12:19:07 2021

@author: Kunal
"""

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
import _thread as thread
import configparser
from itertools import permutations, combinations
import collections

config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['DEFAULT']['api_key']
api_secret = config['DEFAULT']['api_secret']
username = config['DEFAULT']['username']
password = config['DEFAULT']['password']
pin = config['DEFAULT']['pin']
saveloc = config['DEFAULT']['saveloc']
discordtoken = config['DEFAULT']['discordtoken']
u = int(config['DEFAULT']['maxstrike'])
l = int(config['DEFAULT']['minstrike'])
now = datetime.datetime.now()
writing = {'thread': 0}
someticks = []
last_tick = {}
niftyhis = pd.read_csv('nifty1jan2011-23mar2021.csv')
tradedict = {}
ltp = {}
pnls = {}
spreadcalls = {}
spreadputs = {}
callspreadev = {}
putspreadev = {}
condorev = {}
optionsev = {}
resultdf = pd.DataFrame()
showcondor = {}

tickdf = pd.DataFrame(
    columns=['timestamp', 'ltp', 'last_quantity', 'average_price', 'volume', 'buy_quantity', 'sell_quantity', 'oi',
             'oi_day_high', 'oi_day_low', 'depth'])


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

# Tokens to subscribe
# Minimum and Maximum strike price to Monitor Options

instru = kite.instruments()
TOKENS = []
nifty50 = list(pd.read_csv("ind_nifty50list.csv")['Symbol'])
# Appends Nifty 50 Stocks to the Ticker (TOKENS)
# for ins in instru:
#    if ins['tradingsymbol'] in nifty50:
#        TOKENS.append(ins['instrument_token'])
options = []


def tradables(instru, lstrike, ustrike):
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100
    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            days = (ins['expiry'] - now.date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days

    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            daystoexpiry = (ins['expiry'] - now.date()).days
            if ins['instrument_type'] == 'FUT':
                if daystoexpiry == expfut:
                    filtered.append(ins)
            elif daystoexpiry == expopt and ins['strike'] >= lstrike and ins['strike'] <= ustrike:
                filtered.append(ins)
    return filtered


def niftyinstruments():
    filtered = []
    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['exchange'] == 'NFO' and (
                ins['expiry'] - datetime.datetime.now().date()).days < 40:
            filtered.append(ins)
    return filtered


instruments = tradables(instru, l, u)
allinstruments = kite.instruments()
ninstruments = niftyinstruments()
for ins in instruments:
    TOKENS.append(ins['instrument_token'])


def stp(symbol, parameter):
    for ins in allinstruments:
        if ins['tradingsymbol'] == symbol:
            return ins[parameter]


def nstp(symbol, parameter):
    for ins in ninstruments:
        if ins['tradingsymbol'] == symbol:
            return ins[parameter]


def sttp(symbol, parameter):
    for ins in instruments:
        if ins['tradingsymbol'] == symbol:
            return ins[parameter]


def historicalresults(df, n):
    pcpd = list(df['pcpd'])

    results = []
    for i in range(0, (len(pcpd) - n - 1)):
        temp = 0
        for k in range(i, i + n):
            temp = temp + pcpd[k]
        results.append(temp)
    return results


def findresult(positions, expiry):
    poslist = positions.keys()
    pnl = 0
    for pos in poslist:

        if 'CE' in pos:
            strike = sttp(pos, 'strike')
            diff = expiry - strike
            if diff <= 0:
                pnl = pnl - (last_tick[sttp(pos, 'instrument_token')]['last_price'] * positions[pos])
            else:
                if positions[pos] > 0:
                    pnl = pnl + ((expiry - strike - last_tick[sttp(pos, 'instrument_token')]['last_price']) * positions[
                        pos])
                else:
                    pnl = pnl - ((last_tick[sttp(pos, 'instrument_token')]['last_price'] - expiry + strike) * positions[
                        pos])
        elif 'PE' in pos:
            strike = sttp(pos, 'strike')
            diff = expiry - strike
            if diff >= 0:
                pnl = pnl - (last_tick[sttp(pos, 'instrument_token')]['last_price'] * positions[pos])
            else:
                if positions[pos] > 0:
                    pnl = pnl + ((strike - expiry - last_tick[sttp(pos, 'instrument_token')]['last_price']) * positions[
                        pos])
                else:
                    pnl = pnl - ((expiry - strike + last_tick[sttp(pos, 'instrument_token')]['last_price']) * positions[
                        pos])
        elif 'FUT' in pos:
            diff = expiry - last_tick[sttp(pos, 'instrument_token')]['last_price']
            pnl = pnl + diff * positions[pos]
    return pnl


def find_existing_positions_result(existing, expiry):
    existing = existing['net']
    niftyexisting = []
    for pos in existing:
        if 'NRML' == pos['product'] and nstp(pos['tradingsymbol'], 'name') == 'NIFTY' and pos['quantity'] != 0 and pos[
            'overnight_quantity'] != 0 and pos['average_price'] != 0:
            niftyexisting.append(pos)
    positions = {}
    avgprices = {}
    for pos in niftyexisting:
        positions[pos['tradingsymbol']] = int(pos['buy_quantity']) - int(pos['sell_quantity'])
        avgprices[pos['tradingsymbol']] = float(pos['average_price'])
    poslist = positions.keys()

    pnl = 0
    for pos in poslist:

        if 'CE' in pos:
            strike = nstp(pos, 'strike')
            diff = expiry - strike
            if diff <= 0:
                pnl = pnl - (avgprices[pos] * positions[pos])
            else:
                if positions[pos] > 0:
                    pnl = pnl + ((expiry - strike - avgprices[pos]) * positions[pos])
                else:
                    pnl = pnl - ((avgprices[pos] - expiry + strike) * positions[pos])
        elif 'PE' in pos:
            strike = nstp(pos, 'strike')
            diff = expiry - strike
            if diff >= 0:
                pnl = pnl - (avgprices[pos] * positions[pos])
            else:
                if positions[pos] > 0:
                    pnl = pnl + ((strike - expiry - avgprices[pos]) * positions[pos])
                else:
                    pnl = pnl - ((expiry - strike + avgprices[pos]) * positions[pos])
        elif 'FUT' in pos:
            diff = expiry - avgprices[pos]
            pnl = pnl + diff * positions[pos]
    return pnl

tickdf = pd.DataFrame(
    columns=['token', 'timestamp', 'ltp', 'last_quantity', 'average_price', 'volume', 'buy_quantity', 'sell_quantity',
             'oi', 'oi_day_high', 'oi_day_low', 'depth'])

# # setting access token ti kite connect object
kite.set_access_token(access_token_zerodha)

# time at which code starts
STARTTIME = datetime.datetime.now()
# time at which code ends
ENDTIME = STARTTIME + datetime.timedelta(seconds=10)  # datetime.datetime.now().replace(hour=14,minute=7,second=0)
etime = datetime.datetime(now.year, now.month, now.day, 15, 30, 0)
print(ENDTIME)
# database to store last traded price DATABASE = {token:{'timestamp':[],'ltp':[],'last_quantity':[],'average_price':[
# ],'volume':[],'buy_quantity':[],'sell_quantity':[],'oi':[],'oi_day_high':[],'oi_day_low':[],'depth':[]} for token
# in TOKENS}

# waits till start time
while datetime.datetime.now() < STARTTIME:
    pass

# kite ticker object to recieve data from zerodha
kws = KiteTicker(api_key, access_token_zerodha)

# function to run when data is coming from zerodha
monitoring = {}


def ibft(token):
    if token in monitoring.keys():
        return monitoring[token]
    else:
        monitoring[token] = ibt(token)
        return monitoring[token]


def ibt(token):
    for ins in instru:
        if ins['instrument_token'] == token:
            return ins['tradingsymbol']


def sendviathread(merged_df, x):
    merged_df.to_csv(saveloc + ibft(x) + '_' + str(datetime.datetime.now().date()) + '.csv')
    writing['thread'] = 0


def writetocsv(merged_df, x):
    thread.start_new_thread(sendviathread, (merged_df, x))


def refreshresultdf(l, u, exp, update):
    niftysim = []
    #    exp=int(input("Enter number of days to expiry for simulation"))
    resultdf = pd.DataFrame()
    resultdf["Spot"] = niftysim
    resultdf.index = niftysim
    resultdf.pop("Spot")
    for ins in instruments:
        if ins['instrument_type'] == 'FUT' and ins['name'] == 'NIFTY':
            print("Future Expiry: ", ins['expiry'])
            niftyclose = last_tick[ins['instrument_token']]['last_price']
            break

    for ins in instruments:
        if ins['instrument_type'] != 'FUT':
            print("Options Expiry: ", ins['expiry'])
            break
    for pc in (historicalresults(niftyhis, exp)[1:]):
        niftysim.append(niftyclose + (niftyclose * pc))
    niftysim.sort()
    call = {}
    put = {}
    for ins in instruments:
        if ins['strike'] >= l and ins['strike'] <= u:
            if ins['instrument_type'] == 'CE':
                call[ins['tradingsymbol']] = last_tick[ins['instrument_token']]['last_price']
            elif ins['instrument_type'] == 'PE':
                put[ins['tradingsymbol']] = last_tick[ins['instrument_token']]['last_price']
    for c in call:
        temp = []
        for spot in niftysim:
            temp.append(findresult({c: 75}, spot))
        resultdf[c] = temp

    for c in put:
        temp = []
        for spot in niftysim:
            temp.append(findresult({c: 75}, spot))
        resultdf[c] = temp
    # Adjusting for Existing Positions
    if update:
        print("Adjusting for Existing Positions")
        existing = kite.positions()
        print("Adding Existing Positions")
        temp = []
        for spot in niftysim:
            temp.append(find_existing_positions_result(existing, spot))
        resultdf['positions'] = temp
    resultdf['spot'] = niftysim
    return niftysim, resultdf


def sortdict(dicti, order):
    if order == 'a':
        return {k: v for k, v in sorted(dicti.items(), key=lambda item: item[1])}
    else:
        return {k: v for k, v in sorted(dicti.items(), key=lambda item: item[1], reverse=True)}


def findstrike(text):
    return int(text[len(text) - 7:len(text) - 2])


def updatecondor(l, u, exp, fresh, update):
    optionsev = {}
    niftysim, resultdf = refreshresultdf(l, u, exp, update)
    print("Updated Resultdf")
    for ins in resultdf.columns.tolist():
        if ins != 'positions' and ins != 'spot':
            optionsev[ins] = resultdf[ins].sum()

    optionsev = sortdict(optionsev, 'd')
    lc5, sc5, lp5, sp5 = [], [], [], []
    for key in optionsev:
        if 'CE' in key:
            if len(lc5) < 20:
                lc5.append(key)
        else:
            if len(lp5) < 20:
                lp5.append(key)
    optionsev = sortdict(optionsev, 'a')
    for key in optionsev:
        if 'CE' in key:
            if len(sc5) < 20:
                sc5.append(key)
        else:
            if len(sp5) < 20:
                sp5.append(key)

    condorev = {}
    condorlist = []
    callspreads = []
    putspreads = []
    showcondor = {}
    curniftysymb = 'NIFTY21APRFUT'
    for lc in lc5:
        for sc in sc5:
            if findstrike(lc) > findstrike(sc):
                if findstrike(sc) > last_tick[nstp(curniftysymb, 'instrument_token')]['last_price']:
                    callspreads.append([lc, sc])
            else:
                if findstrike(lc) > last_tick[nstp(curniftysymb, 'instrument_token')]['last_price']:
                    callspreads.append([sc, lc])
    for lp in lp5:
        for sp in sp5:
            if findstrike(lp) < findstrike(sp):
                if findstrike(sp) < last_tick[nstp(curniftysymb, 'instrument_token')]['last_price']:
                    putspreads.append([lp, sp])
            else:
                if findstrike(lp) < last_tick[nstp(curniftysymb, 'instrument_token')]['last_price']:
                    putspreads.append([sp, lp])
    for cspread in callspreads:
        for pspread in putspreads:
            condorlist.append([cspread[0], cspread[1], pspread[0], pspread[1]])
    n = 0
    if fresh:
        for condor in condorlist:
            if condor[0] != condor[1] and condor[2] != condor[3]:
                resultdf['[' + condor[0] + '-' + condor[1] + ']' + ' & ' + '[' + condor[2] + '-' + condor[3] + ']'] = \
                    resultdf[condor[0]] - resultdf[condor[1]] + resultdf[condor[2]] - resultdf[condor[3]]
                condorev['[' + condor[0] + '-' + condor[1] + ']' + ' & ' + '[' + condor[2] + '-' + condor[3] + ']'] = \
                    resultdf[
                        '[' + condor[0] + '-' + condor[1] + ']' + ' & ' + '[' + condor[2] + '-' + condor[3] + ']'].sum()
                s1 = int(condor[0][len(condor[0]) - 7:len(condor[0]) - 2])
                s2 = int(condor[1][len(condor[1]) - 7:len(condor[1]) - 2])
                s3 = int(condor[2][len(condor[2]) - 7:len(condor[2]) - 2])
                s4 = int(condor[3][len(condor[3]) - 7:len(condor[3]) - 2])
                showcondor[n] = [s1, s2, s3, s4]
                #        print([s1,s2,s3,s4])
                n = n + 1
            condorev = dict(sorted(condorev.items(), key=lambda item: item[1], reverse=1))
        return condorev, showcondor,resultdf
    else:
        print("Finding Condors for Exising Positions")
        for condor in condorlist:
            if condor[0] != condor[1] and condor[2] != condor[3]:
                resultdf['[' + condor[0] + '-' + condor[1] + ']' + ' & ' + '[' + condor[2] + '-' + condor[3] + ']'] = \
                    resultdf[condor[0]] - resultdf[condor[1]] + resultdf[condor[2]] - resultdf[condor[3]] + resultdf[
                        'positions']
                condorev['[' + condor[0] + '-' + condor[1] + ']' + ' & ' + '[' + condor[2] + '-' + condor[3] + ']'] = \
                    resultdf[
                        '[' + condor[0] + '-' + condor[1] + ']' + ' & ' + '[' + condor[2] + '-' + condor[3] + ']'].sum()
                s1 = int(condor[0][len(condor[0]) - 7:len(condor[0]) - 2])
                s2 = int(condor[1][len(condor[1]) - 7:len(condor[1]) - 2])
                s3 = int(condor[2][len(condor[2]) - 7:len(condor[2]) - 2])
                s4 = int(condor[3][len(condor[3]) - 7:len(condor[3]) - 2])
                showcondor[n] = [s1, s2, s3, s4]
                #        print([s1,s2,s3,s4])
                n = n + 1
                condorev = dict(sorted(condorev.items(), key=lambda item: item[1], reverse=1))
        print(len(condorev))
        return condorev, showcondor, resultdf


def on_ticks(ws, ticks):
    # print("receiving ",str(len(ticks)))
    for x in ticks:
        last_tick[x['instrument_token']] = x


# function to run when connection is established to zerodha
def on_connect(ws, response):
    print("Connected")

    # Callback on successful connect.
    # ws.subscribe(TOKENS)
    # ws.set_mode(ws.MODE_FULL, TOKENS)


# funcion to run on connection close
def on_close(ws, code, reason):
    # On connection close try to reconnect
    pass

for token in TOKENS:
    ibft(token)
# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=1)
time.sleep(2)
# condorev, showcondor,resultdf = updatecondor(l, u, 2, True, True)

# try:
#    order_id = kite.place_order(tradingsymbol="INFY",
#                                exchange=kite.EXCHANGE_NSE,
#                                transaction_type=kite.TRANSACTION_TYPE_BUY,
#                                quantity=1,
#                                order_type=kite.ORDER_TYPE_MARKET,
#                                product=kite.PRODUCT_MIS,
#                                variety="regular")
#
#    print("Order placed. ID is: {}".format(order_id))
# except Exception as e:
#    print("Order placement failed: {}".format(e.message))
