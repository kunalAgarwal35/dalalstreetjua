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
import configparser
import pickle
import _thread as thread

config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['DEFAULT']['api_key']
api_secret = config['DEFAULT']['api_secret']
username = config['DEFAULT']['username']
password = config['DEFAULT']['password']
pin = config['DEFAULT']['pin']
now = datetime.datetime.now()
last_tick={}
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
def dumptick():
    pickle.dump(last_tick, open('ticks/' + datetime.datetime.now().strftime("%m%d%Y-%H%M%S"), "wb"))
def on_ticks(ws, ticks):
    # print("receiving ",str(len(ticks)))
    for x in ticks:
        last_tick[x['instrument_token']] = x
        thread.start_new_thread(dumptick, ())


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

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=1)

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