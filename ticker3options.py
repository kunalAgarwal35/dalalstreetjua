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
import sensibull
from pyxll import xl_func
config=configparser.ConfigParser()
config.read('config.ini')

api_key = config['DEFAULT']['api_key']
api_secret = config['DEFAULT']['api_secret']
username = config['DEFAULT']['username']
password = config['DEFAULT']['password']
pin = config['DEFAULT']['pin']
saveloc = config['DEFAULT']['saveloc']
now = datetime.datetime.now()
writing={'thread':0}
someticks=[]
last_tick={}
niftyhis=pd.read_csv('nifty1jan2011-23mar2021.csv')
tradedict={}
ltp={}
pnls={}
spreadcalls={}
spreadputs={}
callspreadev={}
putspreadev={}
condorev={}
optionsev={}
resultdf=pd.DataFrame()
showcondor={}
volumetillnow={}
minvolavg={}
startoi={}
volmin=datetime.datetime.now()
tickdf=pd.DataFrame(columns=['timestamp','ltp','last_quantity','average_price','volume','buy_quantity','sell_quantity','oi','oi_day_high','oi_day_low','depth'])
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
    
    passwordField = getCssElement( driver , "input[placeholder=Password]" )
    passwordField.send_keys( password )
    
    userNameField = getCssElement( driver , "input[placeholder='User ID (eg: AB0001)']" )
    userNameField.send_keys( username )
    
    loginButton = getCssElement( driver , "button[type=submit]" )
    loginButton.click()
    
    WebDriverWait(driver, 100).until(EC.presence_of_element_located((By.CLASS_NAME, 'twofa-value')))
    pinField = driver.find_element_by_class_name('twofa-value').find_element_by_xpath(".//input[1]")
    pinField.send_keys( pin )
    
    loginButton = getCssElement( driver , "button[type=submit]" )
    loginButton.click()
    
    while True:
        try:
            request_token=furl(driver.current_url).args['request_token'].strip()
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
access_token_zerodha = open("access_token.txt",'r').read()


# #creating kite connect object
kite = KiteConnect(api_key=api_key)

# Tokens to subscribe
#Minimum and Maximum strike price to Monitor Options
maxstrike=15500
minstrike=14000
instru=kite.instruments()
TOKENS = []
nifty50=list(pd.read_csv("ind_nifty50list.csv")['Symbol'])
#Appends Nifty 50 Stocks to the Ticker (TOKENS)
#for ins in instru:
#    if ins['tradingsymbol'] in nifty50:
#        TOKENS.append(ins['instrument_token'])
options=[]

def tradables(instru,lstrike,ustrike):
    filtered=[]
    types=['CE','PE','FUT']
    expfut=100
    expopt=100
    for ins in instru:
        if ins['name']=='NIFTY'and ins['instrument_type'] in types:
            days=(ins['expiry']-now.date()).days
            if ins['instrument_type']=='FUT' and days<expfut:
                expfut=days
            elif days<expopt:
                expopt=days
                
    for ins in instru:
        if ins['name']=='NIFTY'and ins['instrument_type'] in types:
            daystoexpiry=(ins['expiry']-now.date()).days
            if ins['instrument_type']=='FUT':
                if daystoexpiry==expfut:
                    filtered.append(ins)
            elif daystoexpiry==expopt and ins['strike']>=lstrike and ins['strike']<=ustrike:
                filtered.append(ins)
    return filtered
def nifty50stockfutures():
    filtered=[]
    types=['FUT']
    expfut=100
    nifty50=list(pd.read_csv("ind_nifty50list.csv")['Symbol'])
    nifty50.append('NIFTY')
    instru=kite.instruments()
    for ins in instru:
        if ins['name']=='NIFTY'and ins['instrument_type'] in types:
            days=(ins['expiry']-now.date()).days
            if ins['instrument_type']=='FUT' and days<expfut:
                expfut=days

    for ins in instru:
        if ins['name'] in nifty50 and ins['instrument_type'] in types:
            daystoexpiry=(ins['expiry']-now.date()).days
            if ins['instrument_type']=='FUT':
                if daystoexpiry==expfut:
                    filtered.append(ins)
    
    return filtered 
    
            
instruments=tradables(instru,minstrike,maxstrike)

TOKENS=[]

UNSUBTOKENS=[]
for ins in instruments:
    TOKENS.append(ins['instrument_token'])

def change_tokens_to_n50fut():
    global TOKENS
    global UNSUBTOKENS
    oldtokens=TOKENS
    n50=nifty50stockfutures()
    for token in oldtokens:
        UNSUBTOKENS.append(token)
    newtokens=[]
    for ins in n50:
        newtokens.append(ins['instrument_token'])
    TOKENS=newtokens
        

def stp(symbol,parameter):
    for ins in instruments:
        if ins['tradingsymbol']==symbol:
            return ins[parameter]
        
def historicalresults(df,n):
    pcpd=list(df['pcpd'])
    
    results=[]
    for i in range(0,(len(pcpd)-n-1)):
        temp=0
        for k in range(i,i+n):
            temp=temp+pcpd[k]
        results.append(temp)
    return results

def findresult(positions,expiry):
    poslist=positions.keys()
    pnl=0
    for pos in poslist:

        if 'CE' in pos:
            strike=stp(pos,'strike')
            diff=expiry-strike
            if diff<=0:
                pnl=pnl-(last_tick[stp(pos,'instrument_token')]['last_price']*positions[pos])
            else:
                if positions[pos]>0:
                    pnl=pnl+((expiry-strike-last_tick[stp(pos,'instrument_token')]['last_price'])*positions[pos])
                else:
                    pnl=pnl-((last_tick[stp(pos,'instrument_token')]['last_price']-expiry+strike)*positions[pos])
        elif 'PE' in pos:
            strike=stp(pos,'strike')
            diff=expiry-strike
            if diff>=0:
                pnl=pnl-(last_tick[stp(pos,'instrument_token')]['last_price']*positions[pos])
            else:
                if positions[pos]>0:
                    pnl=pnl+((strike-expiry-last_tick[stp(pos,'instrument_token')]['last_price'])*positions[pos])
                else:
                    pnl=pnl-((expiry-strike+last_tick[stp(pos,'instrument_token')]['last_price'])*positions[pos])
        elif 'FUT' in pos:
            diff=expiry-last_tick[stp(pos,'instrument_token')]['last_price']
            pnl=pnl+diff*positions[pos]
    return pnl
tradedict={'CE1500':-75,'CE1000':75}
prices={'PE1000':200,'PE1800':650,'CE1500':200,'CE1000':650}
def findresultmock(positions,expiry):
    poslist=positions.keys()
    pnl=0
    for pos in poslist:

        if 'CE' in pos:
            strike=int(pos.replace('CE',''))
            diff=expiry-strike
            if diff<=0:
                pnl=pnl-(prices[pos]*positions[pos])
            else:
                if positions[pos]>0:
                    pnl=pnl+((expiry-strike-prices[pos])*positions[pos])
                else:
                    pnl=pnl-((prices[pos]-expiry+strike)*positions[pos])
        elif 'PE' in pos:
            strike=int(pos.replace('PE',''))
            diff=expiry-strike
            if diff>=0:
                pnl=pnl-(prices[pos]*positions[pos])
            else:
                if positions[pos]>0:
                    pnl=pnl+((strike-expiry-prices[pos])*positions[pos])
                else:
                    pnl=pnl-((expiry-strike+prices[pos])*positions[pos])
        elif 'FUT' in pos:
            diff=expiry-prices[pos]
            pnl=pnl+diff*positions[pos]
    return pnl
#
#def filterevs(inp,l,u):
#    strings=[]
#    
#    while i in range(l,u):
#        
#        
#    if inp=='CE':
#        for key in list(callspreadev.keys()):
            
tickdf=pd.DataFrame(columns=['token','timestamp','ltp','last_quantity','average_price','volume','buy_quantity','sell_quantity','oi','oi_day_high','oi_day_low','depth'])


# # setting access token ti kite connect object
kite.set_access_token(access_token_zerodha)

# time at which code starts
STARTTIME = datetime.datetime.now()
#time at which code ends
ENDTIME = STARTTIME + datetime.timedelta(seconds=10)#datetime.datetime.now().replace(hour=14,minute=7,second=0)
etime=datetime.datetime(now.year,now.month,now.day,15,30,0)
print(ENDTIME)
# database to store last traded price
#DATABASE = {token:{'timestamp':[],'ltp':[],'last_quantity':[],'average_price':[],'volume':[],'buy_quantity':[],'sell_quantity':[],'oi':[],'oi_day_high':[],'oi_day_low':[],'depth':[]} for token in TOKENS}

#waits till start time
while datetime.datetime.now()<STARTTIME:
    pass

#kite ticker object to recieve data from zerodha
kws = KiteTicker(api_key, access_token_zerodha)

#function to run when data is coming from zerodha
monitoring={}
def ibft(token):
    if token in monitoring.keys():
        return monitoring[token]
    else:
        monitoring[token]=ibt(token)
        return monitoring[token]
    
def ibt(token):
    for ins in instru:
        if ins['instrument_token']==token:
            return ins['tradingsymbol']

    
def sendviathread(merged_df,x):
    merged_df.to_csv(saveloc+ibft(x)+'_'+str(datetime.datetime.now().date())+'.csv')
    writing['thread']=0
    
def writetocsv(merged_df,x):
    thread.start_new_thread(sendviathread, (merged_df,x))
def refreshresultdf(l,u,exp):
    niftysim=[]
#    exp=int(input("Enter number of days to expiry for simulation"))
    resultdf=pd.DataFrame()
    resultdf["Spot"]=niftysim
    resultdf.index=niftysim
    resultdf.pop("Spot")
    for ins in instruments:
        if ins['instrument_type']=='FUT' and ins['name']=='NIFTY':
            print("Future Expiry: ",ins['expiry'])
            niftyclose=last_tick[ins['instrument_token']]['last_price']
            break
        
    for ins in instruments:
        if ins['instrument_type']!='FUT':
            print("Options Expiry: ",ins['expiry'])
            break
    for pc in (historicalresults(niftyhis,exp)[1:]):
        niftysim.append(niftyclose+(niftyclose*pc))
    call={}
    put={}
    for ins in instruments:
        if ins['strike']>=l and ins['strike']<=u:
            if ins['instrument_type']=='CE':
                call[ins['tradingsymbol']]=last_tick[ins['instrument_token']]['last_price']
            elif ins['instrument_type']=='PE':
                put[ins['tradingsymbol']]=last_tick[ins['instrument_token']]['last_price']
    for c in call:
        temp=[]
        for spot in niftysim:
            temp.append(findresult({c:1},spot))
        resultdf[c]=temp
        
    for c in put:
        temp=[]
        for spot in niftysim:
            temp.append(findresult({c:1},spot))
        resultdf[c]=temp
    return niftysim,resultdf

def sortdict(dicti,order):
    if order=='a':
        return {k: v for k, v in sorted(dicti.items(), key=lambda item: item[1])}
    else:
        return {k: v for k, v in sorted(dicti.items(), key=lambda item: item[1],reverse=True)}
        
def updateall(l,u,exp):
    optionsev={}
    niftysim,resultdf=refreshresultdf(l,u,exp)
    for ins in resultdf.columns.tolist():
        optionsev[ins]=resultdf[ins].sum()

    optionsev=sortdict(optionsev,'d')
    lc5,sc5,lp5,sp5=[],[],[],[]
    for key in optionsev:
        if 'CE' in key:
             if len(lc5)<5:
                 lc5.append(key)
        else:
            if len(lp5)<5:
                lp5.append(key)
    optionsev=sortdict(optionsev,'a')
    for key in optionsev:
        if 'CE' in key:
             if len(sc5)<5:
                 sc5.append(key)
        else:
            if len(sp5)<5:
                sp5.append(key)
    
    condorev={}
    condorlist=[]
    callspreads=[]
    putspreads=[]
    showcondor={}
    for lc in lc5:
        for sc in sc5:
            callspreads.append([lc,sc])
    for lp in lp5:
        for sp in sp5:
            putspreads.append([lp,sp])
    for cspread in callspreads:
        for pspread in putspreads:
            condorlist.append([cspread[0],cspread[1],pspread[0],pspread[1]])
    n=0
    for condor in condorlist:
        resultdf['['+condor[0]+'-'+condor[1]+']'+ ' & ' + '['+condor[2]+'-'+condor[3]+']']=resultdf[condor[0]]-resultdf[condor[1]]+resultdf[condor[2]]-resultdf[condor[3]]
        condorev['['+condor[0]+'-'+condor[1]+']'+ ' & ' + '['+condor[2]+'-'+condor[3]+']']=resultdf['['+condor[0]+'-'+condor[1]+']'+ ' & ' + '['+condor[2]+'-'+condor[3]+']'].sum()
        s1=int(condor[0][len(condor[0])-7:len(condor[0])-2])
        s2=int(condor[1][len(condor[1])-7:len(condor[1])-2])
        s3=int(condor[2][len(condor[2])-7:len(condor[2])-2])
        s4=int(condor[3][len(condor[3])-7:len(condor[3])-2])
        showcondor[n]=[s1,s2,s3,s4]
#        print([s1,s2,s3,s4])
        n=n+1
    condorev=dict(sorted(condorev.items(), key=lambda item: item[1],reverse=1))
    return condorev,showcondor
def findstrike(text):
    return int(text[len(text)-7:len(text)-2])
def updatecondor(l,u,exp):
    optionsev={}
    niftysim,resultdf=refreshresultdf(l,u,exp)
    for ins in resultdf.columns.tolist():
        optionsev[ins]=resultdf[ins].sum()

    optionsev=sortdict(optionsev,'d')
    lc5,sc5,lp5,sp5=[],[],[],[]
    for key in optionsev:
        if 'CE' in key:
             if len(lc5)<15:
                 lc5.append(key)
        else:
            if len(lp5)<15:
                lp5.append(key)
    optionsev=sortdict(optionsev,'a')
    for key in optionsev:
        if 'CE' in key:
             if len(sc5)<15:
                 sc5.append(key)
        else:
            if len(sp5)<15:
                sp5.append(key)
    
    condorev={}
    condorlist=[]
    callspreads=[]
    putspreads=[]
    showcondor={}
    for lc in lc5:
        for sc in sc5:
            if lc>sc:
                if findstrike(sc)>last_tick[stp('NIFTY21APRFUT','instrument_token')]['last_price']:
                    callspreads.append([lc,sc])
            else:
                if findstrike(lc)>last_tick[stp('NIFTY21APRFUT','instrument_token')]['last_price']:
                    callspreads.append([sc,lc])
    for lp in lp5:
        for sp in sp5:
            if lp<sp:
                if findstrike(sp)<last_tick[stp('NIFTY21APRFUT','instrument_token')]['last_price']:
                    putspreads.append([lp,sp])
            else:
                if findstrike(lp)<last_tick[stp('NIFTY21APRFUT','instrument_token')]['last_price']:
                    putspreads.append([sp,lp])
    for cspread in callspreads:
        for pspread in putspreads:
            condorlist.append([cspread[0],cspread[1],pspread[0],pspread[1]])
    n=0
    for condor in condorlist:
        resultdf['['+condor[0]+'-'+condor[1]+']'+ ' & ' + '['+condor[2]+'-'+condor[3]+']']=resultdf[condor[0]]-resultdf[condor[1]]+resultdf[condor[2]]-resultdf[condor[3]]
        condorev['['+condor[0]+'-'+condor[1]+']'+ ' & ' + '['+condor[2]+'-'+condor[3]+']']=resultdf['['+condor[0]+'-'+condor[1]+']'+ ' & ' + '['+condor[2]+'-'+condor[3]+']'].sum()
        s1=int(condor[0][len(condor[0])-7:len(condor[0])-2])
        s2=int(condor[1][len(condor[1])-7:len(condor[1])-2])
        s3=int(condor[2][len(condor[2])-7:len(condor[2])-2])
        s4=int(condor[3][len(condor[3])-7:len(condor[3])-2])
        showcondor[n]=[s1,s2,s3,s4]
#        print([s1,s2,s3,s4])
        n=n+1
    condorev=dict(sorted(condorev.items(), key=lambda item: item[1],reverse=1))
    return condorev,showcondor
def show(n):
    keys=list(condorev.keys())
    print("Check sensibull for ",keys[n])
    [bcs,scs,bps,sps]=showcondor[n]
    sensibull.correctstrikes(bcs,scs,bps,sps)    
def showall():
    for i in range(0,len(showcondor)):
        print('Showing ',i,' of ',len(showcondor))
        show(i)
        input("Enter to continue")
def startandbuildsensibull():
    sensibull.startsensibull()
    time.sleep(2)
    try:
        sensibull.buildnewrandstrat()
    except:
        time.sleep(1)
        try:
            sensibull.buildnewrandstrat()
        except:
            time.sleep(1)
            sensibull.buildnewrandstrat()   

def getminuteohlc(token):
    today=now.date().strftime("%Y-%m-%d")
    data=kite.historical_data(instrument_token=token,from_date=today,to_date=today,interval='minute',oi=1)
    data=pd.DataFrame(data)
    return data
def update_min_volavg():
    if writing['thread']==0:
        writing['thread']=1
        for token in TOKENS:
            data=getminuteohlc(token)
            avg_min_vol=data['volume'].mean()
            minvolavg[token]=avg_min_vol
            writing['thread']=0
    else:
        return
def today_min_volavg(token):
    data=getminuteohlc(token)
    avg_min_vol=data['volume'].mean()
    minvolavg[token]=avg_min_vol
    return avg_min_vol
def today_lastxmin_volavg(token,x):
    data=getminuteohlc(token).iloc[-x:]
    avg_min_vol=data['volume'].mean()
    return avg_min_vol
def oichange(token):
    if token not in startoi.keys():
        data=getminuteohlc(token)
        startoi[token]=data['oi'][0]
    oichange=last_tick[token]['oi']-startoi[token]
    oichange=oichange*100/startoi[token]
    return oichange
def buysell(token):
    return [last_tick[token]['buy_quantity'],last_tick[token]['sell_quantity']]
def updatescreendata():
    global volumetillnow
    global volmin
    global screendata
    if datetime.datetime.now().minute!=volmin:
        volmin=datetime.datetime.now().minute
        for x in last_tick:
            volumetillnow[x]=last_tick[x]['volume']
    volumes1=[]
    oi=[]
    buy=[]
    sell=[]
    for x in TOKENS:
        volumes1.append((last_tick[x]['volume']-volumetillnow[x])/minvolavg[x])
        oi.append(oichange(x))
        bs=buysell(x)
        buy.append(bs[0])
        sell.append(bs[1])
    screendata['V/AvgV']=volumes1
    screendata['OI Change%']=oi
    screendata['Buy_Qty']=buy
    screendata['Sell_Qty']=sell
    screendata['B/S']=screendata['Buy_Qty']/screendata['Sell_Qty']
    thread.start_new_thread(update_min_volavg,())
    thread.start_new_thread(screendatatocsv,())
def screendatatocsv():
    screendata.to_csv('Screener.csv',index=False)
def on_ticks(ws, ticks):
    global TOKENS
    ws.subscribe(TOKENS)
    ws.set_mode(ws.MODE_FULL, TOKENS)
    for x in ticks:
        last_tick[x['instrument_token']]=x
    try:
        updatescreendata()
    except:
        pass
def on_connect(ws, response):
    # Callback on successful connect.
    ws.subscribe(TOKENS)
    ws.set_mode(ws.MODE_FULL, TOKENS)

#funcion to run on connection close
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
# change_tokens_to_n50fut()
kws.connect(threaded=1)

screendata=pd.DataFrame(columns=['tokens','symbols','V/AvgV','OI Change%','Buy_Qty','Sell_Qty'])
screendata['tokens']=TOKENS
symbols=[]
for token in TOKENS:
    symbols.append(ibt(token))
screendata['symbols']=symbols
update_min_volavg()
#for token in TOKENS:
#    print(ibt(token))
    
    
#time.sleep(2)
#l,u,exp=14400,15400,3
#condorev,showcondor=updatecondor(l,u,exp)
#n=0
#startandbuildsensibull()
#for n in range(0,len(showcondor)):
#    count=0
#    for element in showcondor[n]:
#        if element%100 == 0:
#           count=count+1 
#    if count==4:
#        [bcs,scs,bps,sps]=showcondor[n]
#        sensibull.correctstrikes(bcs,scs,bps,sps)
#        input("Enter to move to next")
#    
#[bcs,scs,bps,sps]=showcondor[n]
#sensibull.correctstrikes(bcs,scs,bps,sps)
#for condor in showcondor.values():
#    if condor[0]>15000 and condor[1]>condor[0]:
#        print(condor)
#
#try:
#    order_id = kite.place_order(tradingsymbol="INFY",
#                                exchange=kite.EXCHANGE_NSE,
#                                transaction_type=kite.TRANSACTION_TYPE_BUY,
#                                quantity=1,
#                                order_type=kite.ORDER_TYPE_MARKET,
#                                product=kite.PRODUCT_MIS,
#                                variety="regular")
#
#    print("Order placed. ID is: {}".format(order_id))
#except Exception as e:
#    print("Order placement failed: {}".format(e.message))


        

