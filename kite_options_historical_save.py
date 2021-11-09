import zd
import pickle
import pandas as pd
import numpy as np
import os
import datetime

all_ins = zd.kite.instruments()
curr_ins = {}
for ins in all_ins:
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


def nifty_and_bank_nifty(instru):
    global timeframe
    filtered = []
    unders = ['NIFTY','BANKNIFTY']
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100
    for ins in instru:
        if ins['name'] in unders and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days

    for ins in instru:
        if ins['name'] in unders and ins['instrument_type'] in types:
            daystoexpiry = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT':
                if daystoexpiry == expfut:
                    filtered.append(ins)
            elif daystoexpiry == expopt:
                filtered.append(ins)
    return filtered

def stp(param,type_param,query):
    for ins in all_ins:
        if ins[type_param] == param:
            return ins['query']

def get_historical(instrument_token,fdate,tdate,interv):
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
            dfs.append(pd.DataFrame(zd.kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
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
            dfs.append(pd.DataFrame(zd.kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
callputsandfutures = nifty_and_bank_nifty(all_ins)
fromdate = datetime.datetime(2021,6,1,9,15,0)
todate = datetime.datetime.now()
interv = '5minute'
for ins in callputsandfutures:
    try:
        df = get_historical(ins['instrument_token'],fromdate,todate,interv)
        if ins['instrument_type'] in ['CE','PE'] and ins['name']=='NIFTY':
            fname = str(int(ins['strike']))+ins['instrument_type']
            df.to_csv('current_options/2021-06-24/rec/'+fname+'.csv',index=False)
            print(ins['tradingsymbol'],len(df))
    except:
        print('Error in ',ins['tradingsymbol'])
len(callputsandfutures)

ins