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
file_path='xlwings.xlsx'
sheet2=xw.Book(file_path).sheets['Sheet2']

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



def get_historical(instrument_token,fdate,tdate,interv):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
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
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        return df


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
fdate=datetime.datetime(2011,1,1)
tdate=datetime.datetime(2021,6,8)
dateformat='%Y-%m-%d'
vix=get_historical(vix_instrument['instrument_token'],fdate,tdate, "day")
nifty=get_historical(nifty_50_instrument['instrument_token'],fdate,tdate, "day")
trading_sessions=100

def tradables(instru, lstrike, ustrike):
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
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            daystoexpiry = (ins['expiry'] - now.date()).days
            if ins['instrument_type'] == 'FUT':
                if daystoexpiry == expfut:
                    filtered.append(ins)
            elif daystoexpiry == expopt and ins['strike'] >= lstrike and ins['strike'] <= ustrike:
                filtered.append(ins)
    trading_sessions=expopt+1
    return filtered

def nifty_distribution(vix_min, vix_max, trading_sessions):
    t1=time.time()
    nv=vix.loc[(vix['open']<vix_max) & (vix['open']>vix_min)]
    nnifty=nifty[nifty['date'].isin(nv['date'].to_list())]
    retnifty=pd.DataFrame()
    for index in nnifty.index:
        if index+trading_sessions<len(nifty):
            retnifty=retnifty.append(nifty.loc[index+trading_sessions])
    # print(retnifty)
    # print(nnifty)
    nnifty.reset_index(drop=True,inplace=True)
    retnifty.reset_index(drop=True,inplace=True)
    ret_distribution=(100*np.log(retnifty['close']/nnifty['open'])).dropna()
    # print(ret_distribution)
    # print(time.time()-t1)
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    return ret_distribution

def findbuyresult(instrument,expiry):
    global ltp_by_symbol
    pnl=0
    ltp_by_symbol[instrument['tradingsymbol']]=last_tick[instrument['instrument_token']]['last_price']
    lot_size = instrument['lot_size']
    if instrument['instrument_type']=='CE':
        strike = instrument['strike']
        diff = expiry - strike
        if diff<=0:
            pnl=pnl-(last_tick[instrument['instrument_token']]['last_price']*lot_size)
        else:
            pnl=pnl+((expiry-strike-last_tick[instrument['instrument_token']]['last_price'])*lot_size)
    elif instrument['instrument_type']=='PE':
        strike = instrument['strike']
        diff = expiry - strike
        if diff>=0:
            pnl=pnl-(last_tick[instrument['instrument_token']]['last_price']*lot_size)
        else:
            pnl=pnl+((strike-expiry-last_tick[instrument['instrument_token']]['last_price'])*lot_size)
    elif instrument['instrument_type']=='FUT':
        diff=expiry-last_tick[instrument['instrument_token']]['last_price']
        pnl=pnl+diff*lot_size
    return pnl

def contract_evs(b):
    global last_tick
    vix_now=last_tick[vix_instrument['instrument_token']]['last_price']
    ndis=(nifty_distribution(vix_now*(1-b), vix_now*(1+b), trading_sessions)*nifty_ltp/100)+nifty_ltp
    ev_df=pd.DataFrame()
    ev_df['spot']=ndis
    cev={}
    pev={}
    cpop={}
    ppop={}
    ltp={}
    for ins in all_tradables:
        _=[]
        for item in ev_df['spot']:
            _.append(findbuyresult(ins,item))
        ev_df[ins['tradingsymbol']]=_
        if ins['instrument_type']=='CE':
            cev[ins['tradingsymbol']]=ev_df[ins['tradingsymbol']].mean()
            cpop[ins['tradingsymbol']]=(ev_df[ins['tradingsymbol']]>0).sum()*100/len(ev_df[ins['tradingsymbol']])
        elif ins['instrument_type']=='PE':
            pev[ins['tradingsymbol']] = ev_df[ins['tradingsymbol']].mean()
            ppop[ins['tradingsymbol']] = (ev_df[ins['tradingsymbol']] > 0).sum() * 100 / len(
                ev_df[ins['tradingsymbol']])
        elif ins['instrument_type']=='FUT':
            fut_ev = ev_df[ins['tradingsymbol']].mean()

    return cev,pev,cpop,ppop,fut_ev

def options_ev_to_xlwings():
    b=sheet2.range('J2').value
    cev, pev, cpop, ppop, fut_ev = contract_evs(b)

    calldf = pd.DataFrame()
    calldf['symbols'] = cev.keys()
    calldf['ev'] = cev.values()
    _ = []
    for item in cev.keys():
        _.append(ltp_by_symbol[item])
    calldf['ltp'] = _
    calldf['pop'] = cpop.values()

    putdf = pd.DataFrame()
    putdf['symbols'] = pev.keys()
    putdf['ev'] = pev.values()
    _ = []
    for item in pev.keys():
        _.append(ltp_by_symbol[item])
    putdf['ltp'] = _
    putdf['pop'] = ppop.values()
    sheet2.range('A2').value = calldf.set_index('symbols', drop=True)
    sheet2.range('E2').value = putdf.set_index('symbols', drop=True)
    # sheet2.range('G2', ).value = fut_ev

def dumptick(ticks):
    tick = {}
    for x in ticks:
        tick[x['instrument_token']] = x
    fname = datetime.datetime.now().strftime(iupac)
    pickle.dump(tick, open('optionticks/' + fname, "wb"))
    print(fname, str(len(tick.keys())))

def on_ticks(ws, ticks):
    for x in ticks:
        last_tick[x['instrument_token']]=x
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

from nsetools import Nse
nse=Nse()
# print(nse)
index_codes = nse.get_index_list()
q=nse.get_index_quote('nifty 50')
# print(q)
nifty_ltp=q['lastPrice']
low_strike=sheet2.range('J3').value
high_strike=sheet2.range('J4').value
all_tradables=tradables(allins,nifty_ltp+low_strike,nifty_ltp+high_strike)
TOKENS=[]
for ins in all_tradables:
    TOKENS.append(ins['instrument_token'])
TOKENS.append(vix_instrument['instrument_token'])
TOKENS.append(nifty_50_instrument['instrument_token'])

kws.connect(threaded=1)

time.sleep(2)

# while(1==1):
#     time.sleep(0.3)
#     if sheet2.range('M2').value.lower()=='call':
#         q = nse.get_index_quote('nifty 50')
#         nifty_ltp = q['lastPrice']
#         sheet2.range('M2').value = 'Processing'
#         options_ev_to_xlwings()
#         sheet2.range('M2').value = 'Updated at '
#         sheet2.range('N2').value = datetime.datetime.now()
#         sheet2.range('M3').value = nifty_ltp









