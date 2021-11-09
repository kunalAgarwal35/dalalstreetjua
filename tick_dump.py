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
oi_thresh = float(config['DEFAULT']['oi_thresh'])
price_thresh = float(config['PARAMS']['price_thresh'])
now = datetime.datetime.now()
last_tick = {}
monitoring = []
firsttick = 0
discordtoken = config['DEFAULT']['discordtoken']
wbhk=config['DEFAULT']['wbhk']
param={}
trespassers={}
ts={}
dfs = {}
open_trades = {}
dfs_open_trades = [dfs, open_trades]
client = discord.Client()
nse_equivalent={}
nse_sizing={}
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
def ib153t():
    #updates dictionary for 153 monitored futures
    for token in nifty250fut_tokens():
        for ins in allins:
            if ins['instrument_token']==token:
                ts[token]=ins['tradingsymbol']
def ibnset():
    #updates dictionary for 153 monitored futures
    concerned_tokens=list(nse_equivalent.values())
    for token in concerned_tokens:
        for ins in allins:
            if ins['instrument_token']==token:
                ts[token]=ins['tradingsymbol']
def ibt(x):
    for ins in monitoring:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def ibat(x):
    for ins in allins:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def dumptick(last_tick):
    fname=datetime.datetime.fromtimestamp(time.time()).strftime("%m%d%Y-%H%M%S-%f")
    print('ticks/' + fname)
    pickle.dump(last_tick, open('ticks/' + fname, "wb"))
def dumptrades(d):
    with open('dfs_open_trades.pkl', 'wb') as handle:
        pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)
    handle.close()
def on_ticks(ws, ticks):
    # print('On Ticks Triggered')
    global dfs_open_trades
    for x in ticks:
        last_tick[x['instrument_token']] = x
    t = datetime.datetime.now().strftime("%m%d%Y-%H%M%S")
    # dfs_open_trades = alert(t, last_tick, dfs_open_trades, oi_thresh, price_thresh)
    # print(datetime.datetime.now(),'Currently in Trades: ',len(dfs_open_trades[1]))
    thread.start_new_thread(dumptick, (last_tick,))



    # frontrun()
    # find_trespassers()
def on_connect(ws, response):
    print("Connected")
def on_close(ws, code, reason):
    pass
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


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

ib153t()
ibnset()
kws.connect(threaded=1)
changesubscriptions(nifty250fut_tokens())

#
# with open('june.instruments', 'wb') as handle:
#     pickle.dump(kite.instruments(), handle, protocol=pickle.HIGHEST_PROTOCOL)
# handle.close()

