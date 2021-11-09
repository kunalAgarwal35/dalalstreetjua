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
def pingdiscord(txt):
    webhook = DiscordWebhook(url=wbhk, content=txt)
    response = webhook.execute()

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
def optionsoichange()
def on_ticks(ws, ticks):
    for x in ticks:
        last_tick[x['instrument_token']] = x
    thread.start_new_thread(dumptick, ())
    # thread.start_new_thread(frontrun, ())
    frontrun()
    find_trespassers()



def on_connect(ws, response):
    print("Connected")


def on_close(ws, code, reason):
    pass


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

kws.connect(threaded=1)
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
            print(daystoexpiry)
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
lastbidalert={}
lastaskalert={}
def frontrun():
    t1=time.time()
    tick=last_tick
    for token in tick.keys():
        if 'depth' in tick[token].keys():
            itr = tick[token]['depth']
            for i in range(0,5):
                if itr['buy'][i]['quantity'] > bidstres[token]:
                    if token not in lastalert:
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
                        pingdiscord(tsym+'\n'+url+'\n'+str(itr['buy']))
                    print(tsym+ '---Bids---'+'\n'+url+'\n'+str(itr['buy']))
                    print("Frontrunning time: ",t1-time.time())
                    break
                if itr['sell'][i]['quantity'] > askstres[token]:
                    if token not in lastalert:
                        lastbidalert[token] = time.time()
                        pingdis = 1
                    else:
                        if lastbidalert[token] - time.time() > 120:
                            pingdis = 1
                        else:
                            pingdis = 0

                    tsym = ibt(token)
                    url='https://kite.zerodha.com/chart/ext/ciq/NFO-FUT/'+tsym+'/'+str(token)
                    pdiscord = int(config['PARAMS']['pingdiscord'])
                    if pingdis and pdiscord:
                        pingdiscord(tsym+'\n'+url+'\n'+str(itr['sell']))
                    print(tsym+' ---Asks---'+'\n'+url+'\n'+str(itr['sell']))
                    print("Frontrunning time: ",t1-time.time())
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

    pdiscord=int(config['PARAMS']['pingdiscord'])
    for x in last_tick.keys():
        if x in oldtick:
            diff=100*np.log(last_tick[x]['last_price']/oldtick[x]['last_price'])
            if abs(diff)>float(config['PARAMS']['gaptres']):
                tsym=ibt(x)
                url='https://kite.zerodha.com/chart/web/tvc/NFO-FUT/'+tsym+'/'+str(x)
                if tsym not in trespassers.keys():
                    trespassers[tsym]=last_tick[x]['last_price']
                    print(tsym + ': ' + str(last_tick[x]['last_price']))
                    if pdiscord:
                        pingdiscord(tsym+': ['+str(oldtick[x]['last_price'])+' to '+str(last_tick[x]['last_price'])+']\n'+url)
                else:
                    # print('Old Faithful is back')
                    diff = 100 * np.log(last_tick[x]['last_price']/trespassers[tsym])
                    if abs(diff) > float(config['PARAMS']['gaptres']):
                        trespassers[tsym] = last_tick[x]['last_price']
                        print(tsym+': '+str(last_tick[x]['last_price']))
                        if pdiscord:
                            pingdiscord('Watch again: '+tsym+': ['+str(trespassers[tsym])+' to '+str(last_tick[x]['last_price'])+']\n'+url)
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

changesubscriptions(nifty250fut_tokens())