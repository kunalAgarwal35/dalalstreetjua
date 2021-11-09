# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 23:17:51 2021

@author: Kunal
"""

import zd
import pandas as pd
from datetime import datetime as dt
from dateutil.parser import parse
import dateutil
import os
def login():
    zd.autologin()
    print("Logged in successfully")
    
login()

allinstruments=zd.kite.instruments()
zd.kite.set_access_token(zd.access_token_zerodha)
def returnnifty50stocks():
    csv=pd.read_csv("ind_nifty50list.csv")
    symbols=list(csv['Symbol'])
    nifty50=[]
    for ins in zd.kite.instruments():
        if ins['tradingsymbol'] in symbols:
            nifty50.append(ins)
            symbols.pop(symbols.index(ins['tradingsymbol']))
    
    return nifty50
def returnnifty50fut():
    csv=pd.read_csv("ind_nifty50list.csv")
    symbols=list(csv['Symbol'])
    symbols.append('NIFTY')
    nifty50=[]
    for ins in zd.kite.instruments():
        if ins['name'] in symbols and ins['instrument_type']=='FUT' and ins['expiry'].strftime("%Y-%m-%d")=='2021-04-29':
            nifty50.append(ins)
            try:
                symbols.pop(symbols.index(ins['tradingsymbol']))
            except Exception as e:
                print(e)
                pass
    
    return nifty50
n50=returnnifty50stocks()
#data=zd.kite.historical_data(instrument_token=128022276,from_date='2016-03-31',to_date='2016-05-31',interval='hour')
#data=pd.DataFrame(data)
def savehourlyhistoric(ins):
    token=ins['instrument_token']
    if ins['tradingsymbol']+'.csv' not in os.listdir("historic(Daily)") and ins['tradingsymbol']+'.csv' not in os.listdir("historic[NFO]"):
        a_month = dateutil.relativedelta.relativedelta(months=1)
        fdate="2017-01-02"
        tdate="2017-02-01"
        sdf=zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval='day')
        data=pd.DataFrame(sdf)
        if ins['instrument_type']=='FUT':
            sdf=zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval='day',continuous=True,oi=1)
            data=pd.DataFrame(sdf)
            while(parse(tdate)<dt.now()):
                fdate=(parse(fdate)+a_month).strftime("%Y-%m-%d")
                tdate=(parse(tdate)+a_month).strftime("%Y-%m-%d")
                print("["+ins['tradingsymbol']+"]"+" From: ",fdate," To: ",tdate, " (",str(len(data)),")")
                newdata=zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval='day',continuous=True,oi=1)
                newdata=pd.DataFrame(newdata)
                data=pd.concat([data,newdata],ignore_index=True)
            
            data.to_csv("historic[NFO]/"+ins['tradingsymbol']+".csv")
        else:
            while(parse(tdate)<dt.now()):
                fdate=(parse(fdate)+a_month).strftime("%Y-%m-%d")
                tdate=(parse(tdate)+a_month).strftime("%Y-%m-%d")
                print("["+ins['tradingsymbol']+"]"+" From: ",fdate," To: ",tdate, " (",str(len(data)),")")
                newdata=zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval='day')
                newdata=pd.DataFrame(newdata)
                data=pd.concat([data,newdata],ignore_index=True)
            
            data.to_csv("historic(Daily)/"+ins['tradingsymbol']+".csv")
def saveminhistoric(ins):
    token=ins['instrument_token']
    if ins['tradingsymbol']+'.csv' not in os.listdir("historic(Minute)"):
        a_month = dateutil.relativedelta.relativedelta(months=1)
        fdate="2019-01-02"
        tdate="2019-02-01"
        sdf=zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval='minute')
        data=pd.DataFrame(sdf)
        while(parse(tdate)<dt.now()):
            fdate=(parse(fdate)+a_month).strftime("%Y-%m-%d")
            tdate=(parse(tdate)+a_month).strftime("%Y-%m-%d")
            print("["+ins['tradingsymbol']+"]"+" From: ",fdate," To: ",tdate, " (",str(len(data)),")")
            newdata=zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval='minute')
            newdata=pd.DataFrame(newdata)
            data=pd.concat([data,newdata],ignore_index=True)
        
        data.to_csv("historic(Minute)/"+ins['tradingsymbol']+".csv")

    

    
