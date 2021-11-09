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
def pingdiscord(txt):
    pdiscord = int(config['PARAMS']['pingdiscord'])
    if pdiscord:
        webhook = DiscordWebhook(url=wbhk, content=txt)
        response = webhook.execute()

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
    print('ticks/' + str(time.time()))
    pickle.dump(last_tick, open('ticks/' + str(time.time()), "wb"))
def dumptrades(d):
    with open('dfs_open_trades.pkl', 'wb') as handle:
        pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)
    handle.close()
def nse_equivalents():
    fut_tokens=list(ts.keys())
    names={}
    lots={}
    for item in allins:
        if item['instrument_token'] in fut_tokens:
            names[item['name']]=item['instrument_token']
            lots[item['name']]=item['lot_size']/10
    for item in allins:
        if item['tradingsymbol'] in names and item['instrument_type']=='EQ' and item['segment']=='NSE':
            print("Attaching ",ts[names[item['tradingsymbol']]], 'to ',item['tradingsymbol'])
            nse_equivalent[names[item['tradingsymbol']]]=item['instrument_token']
            nse_sizing[names[item['tradingsymbol']]]=int(lots[item['tradingsymbol']])
def alert(timestamp, data, dfs_open_trades, oi_thresh, price_percent_thresh):
    t1=time.time()
    dfs = dfs_open_trades[0]
    open_trades = dfs_open_trades[1]
    config = configparser.ConfigParser()
    config.read('config.ini')
    trade = int(config['DEFAULT']['trade'])
    cols = ['timestamp', 'buy_depth_price_0', 'sell_depth_price_0', 'last_price', 'oi', 'oic', 'pc', 'ratio', 'per_oic',
            'per_pc']
    start = time.time()
    txt = ''
    for key in list(data.keys()):
        url = 'https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/' + ts[key] + '/' + str(key)
        try:
            if key not in list(dfs.keys()):
                dfs[key] = pd.DataFrame(columns=cols)

            d = {}
            d['timestamp'] = timestamp
            d['buy_depth_price_0'] = data[key]['depth']['buy'][0]['price']
            d['sell_depth_price_0'] = data[key]['depth']['sell'][0]['price']
            d['last_price'] = data[key]['last_price']
            d['oi'] = data[key]['oi']

            if len(dfs[key]) == 0:
                d['oic'] = 0
                d['pc'] = 0
                d['ratio'] = 0
                d['per_oic'] = 0
                d['per_pc'] = 0
                dfs[key] = dfs[key].append(d, ignore_index=True)
                d = {}

            elif len(dfs[key]) == 1 and 100 * abs(np.log(data[key]['oi'] / dfs[key].iloc[0]['oi'])) > oi_thresh:
                d['oic'] = data[key]['oi'] - dfs[key].iloc[0]['oi']
                d['pc'] = data[key]['last_price'] - dfs[key].iloc[0]['last_price']
                d['ratio'] = 0
                d['per_oic'] = 100 * np.log(data[key]['oi'] / dfs[key].iloc[0]['oi'])
                d['per_pc'] = 100 * np.log(data[key]['last_price'] / dfs[key].iloc[0]['last_price'])
                dfs[key] = dfs[key].append(d, ignore_index=True)
                d = {}

            elif len(dfs[key]) == 2 and 100 * abs(np.log(data[key]['oi'] / dfs[key].iloc[1]['oi'])) > oi_thresh:
                d['oic'] = data[key]['oi'] - dfs[key].iloc[1]['oi']
                d['pc'] = data[key]['last_price'] - dfs[key].iloc[1]['last_price']
                pc = dfs[key].iloc[1]['last_price'] - dfs[key].iloc[0]['last_price']
                oic = dfs[key].iloc[1]['oi'] - dfs[key].iloc[0]['oi']
                d['ratio'] = (d['pc'] / d['oic']) / (pc / oic)
                d['per_oic'] = 100 * np.log(data[key]['oi'] / dfs[key].iloc[len(dfs[key]) - 1]['oi'])
                d['per_pc'] = 100 * np.log(data[key]['last_price'] / dfs[key].iloc[len(dfs[key]) - 1]['last_price'])

                dfs[key] = dfs[key].append(d, ignore_index=True)
                d = {}

                n = len(dfs[key])
                p = dfs[key].copy()

                if p.iloc[n - 1]['oic'] > 0 and p.iloc[n - 1]['ratio'] > 1 and abs(
                        p.iloc[n - 1]['per_pc']) > price_percent_thresh:
                    trade_data = {}
                    trade_data['Entry time'] = timestamp
                    trade_data['Token'] = key
                    trade_data['per_oic'] = p.iloc[n - 1]['per_oic']
                    trade_data['per_pc'] = p.iloc[n - 1]['per_pc']
                    trade_data['ratio'] = p.iloc[n - 1]['ratio']

                    if p.iloc[n - 1]['pc'] > 0:
                        trade_data['Type'] = 'long'
                        entry_price = p.iloc[n - 1]['sell_depth_price_0']
                        trade_data['Entry price'] = entry_price
                        print('Entering long at ', trade_data['Entry time'], 'Entry price : ', entry_price)
                        if trade:
                            send_order_nfotoequity('buy',key)
                        txt = txt + 'Alert for Entering LONG\n Entry price = ' + str(entry_price) + '\n' + ts[
                            key] + '\n' + url + '\n\n'
                    if p.iloc[n - 1]['pc'] < 0:
                        # Alert for entering short
                        trade_data['Type'] = 'short'
                        entry_price = p.iloc[n - 1]['buy_depth_price_0']
                        trade_data['Entry Price'] = entry_price
                        print('Entering short at ', trade_data['Entry time'], 'Entry price : ', entry_price)
                        if trade:
                            send_order_nfotoequity('sell', key)
                        txt = txt + 'Alert for Entering SHORT\n Entry price = ' + str(entry_price) + '\n' + ts[
                            key] + '\n' + url + '\n\n'
                    open_trades[key] = trade_data

                elif p.iloc[n - 1]['oic'] > 0 and p.iloc[n - 1]['ratio'] < 1:
                    if key in list(open_trades.keys()):
                        # Alert for exitting
                        if open_trades[key]['Type'] == 'long':
                            exit_price = p.iloc[n - 1]['buy_depth_price_0']
                            if trade:
                                send_order_nfotoequity('sell', key)
                            txt = txt + 'Alert for Exiting LONG\n Exit price = ' + str(exit_price) + '\n' + ts[
                                key] + '\n' + url + '\n\n'
                        else:
                            exit_price = p.iloc[n - 1]['sell_depth_price_0']
                            if trade:
                                send_order_nfotoequity('buy', key)
                            txt = txt + 'Alert for Exiting SHORT\n Exit price = ' + str(exit_price) + '\n' + ts[
                                key] + '\n' + url + '\n\n'

                        print('Exiting ', open_trades[key]['Type'], ' at ', p.iloc[n - 1]['timestamp'], 'Exit price : ',
                              exit_price)

                        open_trades.pop(key)
                dfs[key] = dfs[key].drop(0, axis=0)
            lag = (datetime.datetime.now() - data[key]['timestamp']).seconds
            if lag>3:
                print("Lagging from market by ",lag, " seconds")
        except Exception as e:
            print(e)
    if len(txt) > 1:
        pingdiscord(txt)
    d = [dfs, open_trades]
    #Thread dump open trades
    thread.start_new_thread(dumptrades,(d,))
    end = time.time()
    # print('Time taken : ',time.time()-t1)


    return [dfs, open_trades]
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
    # global dfs_open_trades
    # try:
    #     with open('dfs_open_trades.pkl', 'rb') as handle:
    #         dfs_open_trades = (pickle.load(handle))
    #     handle.close()
    #     print('Picked up from pickle file')
    # except:
    #     print('Initiating Open Trades')
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
def send_order_nfotoequity(type,token):
    if type=='buy':
        try:
            order_id = kite.place_order(tradingsymbol=ts[nse_equivalent[token]],
                                        exchange=kite.EXCHANGE_NSE,
                                        transaction_type=kite.TRANSACTION_TYPE_BUY,
                                        quantity=nse_sizing[token],
                                        order_type=kite.ORDER_TYPE_MARKET,
                                        product=kite.PRODUCT_MIS,
                                        variety='regular')

            print("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            print("Order placement failed: {}".format(e))
    else:
        try:
            order_id = kite.place_order(tradingsymbol=ts[nse_equivalent[token]],
                                        exchange=kite.EXCHANGE_NSE,
                                        transaction_type=kite.TRANSACTION_TYPE_SELL,
                                        quantity=nse_sizing[token],
                                        order_type=kite.ORDER_TYPE_MARKET,
                                        product=kite.PRODUCT_MIS,
                                        variety='regular')

            print("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            print("Order placement failed: {}".format(e))

with open('askthres.pickle', 'rb') as handle:
    askstres = pickle.load(handle)
handle.close()
with open('bidthres.pickle', 'rb') as handle:
    bidstres = pickle.load(handle)
handle.close()

def loadparams():
    config = configparser.ConfigParser()
    config.read('config.ini')
    oi_thresh = config['PARAMS']['oi_thresh']
    price_thresh = config['PARAMS']['price_thresh']
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
    if '$oi_thresh' in txt:
        p=float(txt.replace('$oi_thresh',''))
        config['PARAMS']['oi_thresh']=str(p)
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        configfile.close()
    if '$price_thresh' in txt:
        p=float(txt.replace('$price_thresh',''))
        config['PARAMS']['price_thresh']=str(p)
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
        configfile.close()

@client.event
async def on_ready():
    print('Logged in as {0.user}'.format(client))
    kws.close()
thread.start_new_thread(startbot, ())


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
            loadparams()
        if 'vapgraph' in txt:
            symbol=txt.replace('vapgraph','').replace(' ','')
            keys,values=vapgraph(symbol)
            plt.bar(keys,values)


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

ib153t()
nse_equivalents()
ibnset()
kws.connect(threaded=1)
changesubscriptions(nifty250fut_tokens())

t1=time.time()
ko=kite.orders()
ko=pd.DataFrame(ko)
print(time.time()-t1)
#
# with open('june.instruments', 'wb') as handle:
#     pickle.dump(kite.instruments(), handle, protocol=pickle.HIGHEST_PROTOCOL)
# handle.close()

