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
import matplotlib.pyplot as plt
import xlwings as xw

config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['DEFAULT']['api_key']
api_secret = config['DEFAULT']['api_secret']
username = config['DEFAULT']['username']
password = config['DEFAULT']['password']
pin = config['DEFAULT']['pin']
iupac = "%m%d%Y-%H%M%S-%f"
now = datetime.datetime.now()
ts = {}


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
curr_ins = {}
for ins in allins:
    curr_ins[ins['instrument_token']] = ins
types=[]
segments=[]
exchanges=[]
ticksizes=[]
indices=[]
last_tick={}
ltp_by_symbol={}
for ins in curr_ins.keys():
    if curr_ins[ins]['instrument_type'] not in types:
        types.append(curr_ins[ins]['instrument_type'])
    if curr_ins[ins]['segment'] not in segments:
        segments.append(curr_ins[ins]['segment'])
    if curr_ins[ins]['exchange'] not in exchanges:
        exchanges.append(curr_ins[ins]['exchange'])
    if curr_ins[ins]['tick_size'] not in ticksizes:
        ticksizes.append(curr_ins[ins]['tick_size'])
    if curr_ins[ins]['segment'] == 'INDICES':
        indices.append(curr_ins[ins])

for index in indices:
    if index['name']=='INDIA VIX':
        # print(index)
        vix_instrument=index
        break
for index in indices:
    if index['name']=='NIFTY 50':
        # print(index)
        nifty_50_instrument=index
        break
for index in indices:
    if index['name']=='NIFTY BANK':
        # print(index)
        nifty_bank_instrument=index
        break

def tradables(instru):
    global trading_sessions
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
        if (ins['name'] == 'NIFTY' or ins['name'] == 'BANKNIFTY') and ins['instrument_type'] in types:
            daystoexpiry = (ins['expiry'] - now.date()).days
            if ins['instrument_type'] == 'FUT':
                if daystoexpiry == expfut:
                    filtered.append(ins)
            elif daystoexpiry == expopt:
                filtered.append(ins)
    trading_sessions=expopt+1
    return filtered

def dumptick(ticks):
    tick = {}
    for x in ticks:
        tick[x['instrument_token']] = x
    fname = datetime.datetime.now().strftime(iupac)
    pickle.dump(tick, open('optionticks/' + fname, "wb"))
    print(fname, str(len(tick.keys())))

def on_ticks(ws, ticks):
    # for x in ticks:
    #     last_tick[x['instrument_token']]=x
    thread.start_new_thread(dumptick, (ticks, ))
def on_connect(ws, response):
    # Callback on successful connect.
    # ws.subscribe(TOKENS)
    # ws.set_mode(ws.MODE_FULL, TOKENS)
    kws.subscribe(TOKENS)
    kws.set_mode(kws.MODE_FULL, TOKENS)
    print("Connected")

#funcion to run on connection close
def on_close(ws, code, reason):
    # On connection close try to reconnect
    pass
# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

all_tradables=tradables(allins)
TOKENS=[]
for ins in all_tradables:
    TOKENS.append(ins['instrument_token'])
TOKENS.append(vix_instrument['instrument_token'])
TOKENS.append(nifty_50_instrument['instrument_token'])
TOKENS.append(nifty_bank_instrument['instrument_token'])

kws.connect(threaded=1)








