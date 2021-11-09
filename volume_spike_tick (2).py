import datetime
import pickle, os
import pandas as pd
import numpy as np
import datetime
import xlwings as xw
import threading
import mplfinance as mpl
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

dateformat="%y-%m-%d"
day_tick_dir='dayticks/'
day_list=[i for i in os.listdir(day_tick_dir) if os.stat(day_tick_dir+i).st_size/(1024*1024)>1400]
day_list=[datetime.datetime.strptime(i,dateformat) for i in day_list]
day_list=[i for i in day_list if i>datetime.datetime(2021,4,29)]
file_path='inputoutput.xlsx'
sheet1=xw.Book(file_path).sheets['Sheet1']
ts={}
trading_start_time=datetime.time(9,30,00)
def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename, 'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
    return d
lot_size_dict = get_lot_size_dict('May.instruments')
june_lot_size_dict = get_lot_size_dict('june.instruments')
for token in june_lot_size_dict.keys():
    lot_size_dict[token] = june_lot_size_dict[token]
st={}
for key in ts.keys():
    st[ts[key]]=key

print_df=pd.DataFrame()
vol_ratio_thresh=80
oi_thresh=1
useful=['last_price', 'volume', 'change', 'oi', 'timestamp']
for file in day_list:
    print(file)
    with open(day_tick_dir + file.strftime(dateformat), 'rb') as handle:
        ticksdict = pickle.load(handle)
    handle.close()
    dfs = {}
    vol_sec_ratio={}
    direction={}
    for date in ticksdict.keys():
        if date.time().second==0:
            print(date)
        tick=ticksdict[date]
        for token in tick.keys():
            tick[token]['timestamp'] = date
            poplist=[i for i in tick[token].keys() if i not in useful]
            for key in poplist:
                tick[token].pop(key)
            if token not in dfs.keys():
                dfs[token]=pd.DataFrame()
                dfs[token]=dfs[token].append(tick[token],ignore_index=True)
                vol_sec_ratio[token]=0
                continue
            else:
                if tick[token]['volume'] - dfs[token]['volume'].iloc[-1] > 0:
                    tick[token]['volume_diff'] = tick[token]['volume'] - dfs[token]['volume'].iloc[-1]
                    tick[token]['%change_diff'] = tick[token]['change'] - dfs[token]['change'].iloc[-1]
                    tick[token]['oic'] = tick[token]['oi'] - dfs[token]['oi'].iloc[-1]
                    secdiff = (tick[token]['timestamp'] - dfs[token]['timestamp'].iloc[-1]).seconds
                    tick[token]['vol/sec'] = tick[token]['volume_diff'] / secdiff
                    try:
                        tick[token]['vol_ratio'] = tick[token]['vol/sec'] / dfs[token]['vol/sec'].mean()
                    except:
                        pass
                    dfs[token] = dfs[token].append(tick[token], ignore_index=True)
                if 'vol_ratio' in dfs[token].columns and dfs[token]['timestamp'].iloc[-1].time() > trading_start_time:
                    if dfs[token]['vol_ratio'].iloc[-1] > vol_ratio_thresh:
                            vol_sec_ratio[token] = 1
                            if dfs[token]['last_price'].iloc[-1] > dfs[token]['last_price'].iloc[-2]:
                                direction[token] = 'buy'
                            else:
                                direction[token] = 'sell'
                    if vol_sec_ratio[token] and dfs[token]['oic'].iloc[-1] != 0:
                        if abs(100 * np.log(dfs[token]['oi'].iloc[-1] / dfs[token]['oi'].iloc[-2])) > oi_thresh:
                            newprintrow = dfs[token].iloc[-1].to_dict()
                            newprintrow['comments'] = direction[token]
                            newprintrow['tradingsymbol'] = ts[token]
                            print_df = print_df.append(newprintrow, ignore_index=True)
                            sheet1.range('A1').value = print_df
                        else:
                            vol_sec_ratio[token] = 0
print_df.to_csv("print_df.csv")

print_df=pd.read_csv("print_df.csv")
pdf=print_df.drop_duplicates(subset ="timestamp", keep = "first")
pdf=pdf.drop(columns=["Unnamed: 0"])
# pdf.to_csv("print_df.csv")
nse_equivalent={}
nse_sizing={}
def nse_equivalents(fut_token):
    fut_token
    names={}
    lots={}
    allins=pickle.load(open("june.instruments", 'rb'))
    for item in allins:
        if item['instrument_token'] == fut_token:
            names[item['name']]=item['instrument_token']
            lots[item['name']]=item['lot_size']/10
    for item in allins:
        if item['tradingsymbol'] in names and item['instrument_type']=='EQ' and item['segment']=='NSE':
            print("Attaching ",ts[names[item['tradingsymbol']]], 'to ',item['tradingsymbol'])
            nse_equivalent[names[item['tradingsymbol']]]=item['instrument_token']
            nse_sizing[names[item['tradingsymbol']]]=int(lots[item['tradingsymbol']])


for i in range(0,len(pdf)):
    timedelta=datetime.timedelta(minutes=30)
    todate=((datetime.datetime.strptime(pdf['timestamp'].iloc[i],"%Y-%m-%d %H:%M:%S"))+timedelta).strftime("%Y-%m-%d %H:%M:%S")
    fut_token=st[pdf['tradingsymbol'].iloc[i].replace('MAY','JUN')]
    if fut_token not in nse_equivalent.keys():
        nse_equivalents(fut_token)
    plotdf=pd.DataFrame(kite.historical_data(instrument_token=nse_equivalent[fut_token],from_date=pdf['timestamp'].iloc[i],to_date=todate,interval="minute"))
    plotdf['date'] = plotdf['date'].dt.tz_localize(None)
    plotdf = plotdf.set_index('date')
    filepath='backtests/'+pdf['timestamp'].iloc[i].replace(':','-')
    mpl.plot(plotdf, type='candlestick', title=pdf['tradingsymbol'].iloc[i]+' '+pdf['comments'].iloc[i], show_nontrading=False, volume=True,
             savefig=filepath+'.png', style='charles')
