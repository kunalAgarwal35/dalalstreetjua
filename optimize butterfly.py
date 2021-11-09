# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 08:27:50 2021

@author: Kunal
"""
import pandas as pd
import datetime
import configparser
import os
import zd
from itertools import combinations
config=configparser.ConfigParser()
config.read('config.ini')

niftyhis=pd.read_csv('nifty1jan2011-23mar2021.csv')
tradedict={}
ltp={}
pnls={}
saveloc = config['DEFAULT']['saveloc']
now = datetime.datetime.now()
instru=zd.kite.instruments()

date=str(now.day)
month=str(now.month)
year=str(now.year)

spreadcalls={}
spreadputs={}
callspreadev={}
putspreadev={}


if len(date)==1:
    date='0'+date
if len(month)==1:
    month='0'+month
filedate=year+'-'+month+'-'+date
instruments=tradables(instru,14000,16000)
def stp(symbol,parameter):
    for ins in instruments:
        if ins['tradingsymbol']==symbol:
            return ins[parameter]

#def updateltps():
#    for file in os.listdir(saveloc):
#        if filedate in file:
#            tsymbol=file[:file.index('_')]
#            df=pd.read_csv(saveloc+file)
#            ltps=df['ltp']
#            price=ltps[len(ltps)-1]
#            ltp[tsymbol]=price
def tokentosymbol(token):
    for ins in instruments:
        if ins['instrument_token']==token:
            return ins['tradingsymbol']
    
def updateltps():
    for x in list(last_tick.keys()):
        ltp[tokentosymbol(last_tick[x]['instrument_token'])]=last_tick[x]['last_price']

tradedict={}
tradedict['NIFTY21MAR15000CE']=-75
tradedict['NIFTY21MAR16000CE']=75
tradedict['NIFTY21MAR15000PE']=-75
tradedict['NIFTY21MAR14500PE']=75

testin={14260:-12529,14560:-8029,14960:21971,15000:24971,15580:-18529,16020:-50029}

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
                pnl=pnl-(ltp[pos]*positions[pos])
            else:
                if positions[pos]>0:
                    pnl=pnl+((expiry-strike-ltp[pos])*positions[pos])
                else:
                    pnl=pnl-((ltp[pos]-expiry+strike)*positions[pos])
        elif 'PE' in pos:
            strike=stp(pos,'strike')
            diff=expiry-strike
            if diff>=0:
                pnl=pnl-(ltp[pos]*positions[pos])
            else:
                if positions[pos]>0:
                    pnl=pnl+((strike-expiry-ltp[pos])*positions[pos])
                else:
                    pnl=pnl-((expiry-strike+ltp[pos])*positions[pos])
        elif 'FUT' in pos:
            diff=expiry-ltp[pos]
            pnl=pnl+diff*positions[pos]
    return pnl


updateltps()
for key in tradedict.keys():
    print(key,ltp[key])
  
def tradables(instru,lstrike,ustrike):
    filtered=[]
    types=['CE','PE','FUT']
    for ins in instru:
        if ins['name']=='NIFTY'and ins['instrument_type'] in types:
            daystoexpiry=(ins['expiry']-now.date()).days
            if ins['instrument_type']=='FUT':
                if daystoexpiry<30:
                    filtered.append(ins)
            elif daystoexpiry<7 and ins['strike']>=lstrike and ins['strike']<=ustrike:
                filtered.append(ins)
    return filtered
for key in list(testin.keys()):
    print("Calculated P/L: ",str(findresult(tradedict,key)),', Sensibull Result: ',str(testin[key]))

    
    
def updatespreads():
    niftysim=[]
    exp=1
    instruments=tradables(instru,14000,15500)
    for ins in instruments:
        if ins['instrument_type']!='FUT':
            exp=exp+(ins['expiry']-now.date()).days
            break
    for ins in instruments:
        if ins['instrument_type']=='FUT' and ins['name']=='NIFTY':
            exp=exp+(ins['expiry']-now.date()).days
            if exp<30:
                niftyclose=last_tick[ins['instrument_token']]['last_price']
                break
    for pc in (historicalresults(niftyhis,exp)[1:]):
        niftysim.append(niftyclose+(niftyclose*pc))
    call={}
    put={}
    for ins in instruments:
        if ins['instrument_type']=='CE':
            call[ins['strike']]=last_tick[ins['instrument_token']]['last_price']
        elif ins['instrument_type']=='PE':
            put[ins['strike']]=last_tick[ins['instrument_token']]['last_price']
    callpairs=list(combinations(list(call.keys()),2))
    putpairs=list(combinations(list(put.keys()),2))
    
    for pair in callpairs:
        spreadcalls[str(pair[0])+'-'+str(pair[1])]=call[pair[0]]-call[pair[1]]
    for pair in putpairs:
        spreadputs[str(pair[0])+'-'+str(pair[1])]=put[pair[0]]-put[pair[1]]
    
    for pair in list(spreadcalls.keys()):
        tradedict={}
        strikes=pair.split('-')
        sign='+'
        for strike in strikes:
            for ins in instruments:
                if ins['instrument_type']=='CE' and ins['strike']==float(strike):
                    if sign=='+':
                        sign='-'
                        tradedict[ins['tradingsymbol']]=1
                    else:
                        tradedict[ins['tradingsymbol']]=-1
        res=0
        for i in range(0,len(niftysim)):
            res=res+findresult(tradedict,niftysim[i])
#        print("Niftysim result for \n",tradedict,str(res))
        res=res/len(niftysim)
        callspreadev[pair]=res
    
    for pair in list(spreadputs.keys()):
        tradedict={}
        strikes=pair.split('-')
        sign='+'
        for strike in strikes:
            for ins in instruments:
                if ins['instrument_type']=='PE' and ins['strike']==float(strike):
                    if sign=='+':
                        sign='-'
                        tradedict[ins['tradingsymbol']]=1
                    else:
                        tradedict[ins['tradingsymbol']]=-1
        res=0
        for i in range(0,len(niftysim)):
            res=res+findresult(tradedict,niftysim[i])
#        print("Niftysim result for \n",tradedict,str(res))
        res=res/len(niftysim)
        putspreadev[pair]=res
            
        
                    
        
        
    
            
        
    

            
    

            
    
    
        

