# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 17:45:53 2021

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
import os
config=configparser.ConfigParser()
config.read('config.ini')

api_key = config['DEFAULT']['api_key']
api_secret = config['DEFAULT']['api_secret']
username = config['DEFAULT']['username']
password = config['DEFAULT']['password']
pin = config['DEFAULT']['pin']
now = datetime.datetime.now()
# fuction to wait for elements on page load for selenium
def getCssElement( driver , cssSelector ):
    return WebDriverWait( driver, 100 ).until( EC.presence_of_element_located( ( By.CSS_SELECTOR, cssSelector ) ) )

#function to login to zerodha using selenium
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

access_token_zerodha = open("access_token.txt", 'r').read()

# #creating kite connect object
kite = KiteConnect(api_key=api_key)

# # setting access token ti kite connect object
kite.set_access_token(access_token_zerodha)

# kite ticker object to recieve data from zerodha
kws = KiteTicker(api_key, access_token_zerodha)

def on_ticks(ws, ticks):
    print(len(ticks))


def on_connect(ws, response):
    print("Connected")


def on_close(ws, code, reason):
    pass


def get_historical(instrument_token,fdate,tdate,interv,oi):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    if filename in os.listdir('get_historical'):
        df = pd.read_csv('get_historical/' + filename)
        df['date'] = df[['date']].apply(pd.to_datetime)
        return df
    if interv == "day" and (tdate-fdate).days > 1500:
        fdates=[fdate]
        newtdate=fdate+day1500
        tdates=[newtdate]

        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day1500)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day70 = datetime.timedelta(days=70)
    day1 = datetime.timedelta(days=1)
    if interv == '5minute':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day50 = datetime.timedelta(days=55)
    if interv == 'minute':
        fdates = [fdate]
        newtdate = fdate + day50
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day50)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df

def get_historical_force(instrument_token,fdate,tdate,interv,oi):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    # if filename in os.listdir('get_historical'):
    #     df = pd.read_csv('get_historical/' + filename)
    #     df['date'] = df[['date']].apply(pd.to_datetime)
    #     return df

    if interv == "day" and (tdate-fdate).days > 1500:
        fdates=[fdate]
        newtdate=fdate+day1500
        tdates=[newtdate]

        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day1500)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day70 = datetime.timedelta(days=70)
    day1 = datetime.timedelta(days=1)
    if interv == 'hour':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            try:
                dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                             to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
            except:
                continue
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    if interv == '5minute':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day50 = datetime.timedelta(days=55)
    if interv == 'minute':
        fdates = [fdate]
        newtdate = fdate + day50
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day50)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    if interv == 'hour':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            try:
                dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                             to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
            except:
                continue
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close



