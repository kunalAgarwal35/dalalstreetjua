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


class ThreadSafeDict(dict):
    def __init__(self, *p_arg, **n_arg):
        dict.__init__(self, *p_arg, **n_arg)
        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback):
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
param = {}
ts = {}
yesterday_oi = {}


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


def dumptick(ticks):
    tick = {}
    for x in ticks:
        tick[x['instrument_token']] = x
    fname = datetime.datetime.now().strftime("%m%d%Y-%H%M%S-%f")
    pickle.dump(tick, open('ticks/' + fname, "wb"))
    print(fname, str(len(tick.keys())))


def on_ticks(ws, ticks):
    thread.start_new_thread(dumptick, (ticks, ))


def on_connect(ws, response):
    n50fut = nifty250fut_tokens()
    kws.subscribe(n50fut)
    kws.set_mode(kws.MODE_FULL, n50fut)
    print("Connected")


def on_close(ws, code, reason):
    pass


def nifty250fut_tokens():
    csv = pd.read_csv("nifty250.csv")
    symbols = list(csv['Symbol'])
    tokens = []
    instruments = []
    types = ['FUT']
    expfut = 100
    instru = kite.instruments()
    now = datetime.datetime.now()
    for ins in instru:
        if ins['instrument_type'] == 'FUT' and ins['name'] in symbols:
            days = (ins['expiry'] - now.date()).days
            if days < expfut:
                # print(ins['tradingsymbol'])
                expfut = days
    for ins in instru:
        if ins['name'] in symbols and ins['instrument_type'] == 'FUT':
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
kws.connect(threaded=1)
