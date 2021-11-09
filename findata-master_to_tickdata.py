import os
import pickle
import pandas as pd
import numpy as np
import datetime

maindir = 'findata-master/'
insnames = []
dayformat="%Y-%m-%d"
allins = []
start_date = datetime.datetime(2021,1,1)
for item in os.listdir(maindir):
    if '.' not in item:
        insnames.append(item+'/')
tickdict = {}


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))
for insname in insnames:
    for item in os.listdir(maindir+insname):
        print(item)
        if item == 'futures':
            dayformat = "%Y-%m-%d"
            type = 'FUT'
            segment = 'NFO-FUT'
            exchange = 'NFO'
            tick_size = 0.05
            for csvname in os.listdir(maindir+insname+item):
                expiry = datetime.datetime.strptime(csvname.replace('.csv',''),dayformat)
                if expiry<start_date:
                    continue
                else:
                    ohlc = pd.read_csv(maindir+insname+item+'/'+csvname)
                    ohlc['0'] = pd.to_datetime(ohlc['0'])
                    instoken =insname + item + type + csvname.replace('.csv','')
                    instrument = {'instrument_token':instoken,
                                  'name':insname,
                                  'strike':0,
                                  'instrument_type':type,
                                  'segment':segment,
                                  'exchange':exchange,
                                  'tick_size':tick_size,
                                  'expiry':expiry.date()}
                    if instrument not in allins:
                        allins.append(instrument)
                    for timestamp in ohlc['0'].to_list():
                        if timestamp not in tickdict.keys():
                            tickdict[timestamp] = {}
                        try:
                            close = float(ohlc['4'][ohlc['0']==timestamp])
                            bestbid = close
                            bestask = close
                            oi = int(ohlc['6'][ohlc['0']==timestamp])
                            tick = {'depth':{'buy':[{'price':bestbid}],'sell':[{'price':bestask}]},
                                    'last_price':close,
                                    'oi':oi
                                    }
                            tickdict[timestamp][instoken] = tick
                            # print(tick)
                        except Exception as e:
                            print(e)
        elif item =='index':
            type = 'EQ'
            segment = 'INDICES'
            exchange = 'NSE'
            tick_size = 0.05
            dayformat = "%Y-%m"
            sec59 = datetime.timedelta(seconds=59)
            for csvname in os.listdir(maindir + insname + item):
                expiry = datetime.datetime.strptime(csvname.replace('.csv', ''), dayformat)
                if expiry < start_date:
                    continue
                else:
                    ohlc = pd.read_csv(maindir + insname + item + '/' + csvname)
                    ohlc['0'] = pd.to_datetime(ohlc['0'])
                    ohlc['0'] = ohlc['0'].dt.tz_localize(None)
                    ohlc['0'] = ohlc['0'] + sec59
                    instoken = insname + item + type + csvname.replace('.csv', '')
                    instrument = {'instrument_token': instoken,
                                  'name': insname,
                                  'strike': 0,
                                  'instrument_type': type,
                                  'segment': segment,
                                  'exchange': exchange,
                                  'tick_size': tick_size,
                                  'expiry': expiry.date()}
                    if instrument not in allins:
                        allins.append(instrument)
                    for timestamp in ohlc['0'].to_list():
                        if timestamp not in tickdict.keys():
                            tickdict[timestamp] = {}
                        try:
                            close = float(ohlc['4'][ohlc['0'] == timestamp])
                            bestbid = close
                            bestask = close
                            oi = 0
                            tick = {'depth': {'buy': [{'price': bestbid}], 'sell': [{'price': bestask}]},
                                    'last_price': close,
                                    'oi': oi
                                    }
                            tickdict[timestamp][instoken] = tick
                            # print(tick)
                        except Exception as e:
                            print(e)
        else:
            segment = 'NFO-OPT'
            exchange = 'NFO'
            tick_size = 0.05
            dayformat = "%Y-%m-%d"
            expiries = [datetime.datetime.strptime(item,dayformat) for item in os.listdir(maindir+insname+item)]
            for expiry in expiries:
                if expiry < start_date:
                    continue
                else:
                    folname = expiry.strftime(dayformat)
                    for csvname in os.listdir(maindir+insname+item+'/'+folname):
                        if 'PE' in csvname:
                            type = 'PE'
                        elif 'CE' in csvname:
                            type = 'CE'
                        strike = name_to_strike(csvname.replace('.csv',''))
                        ohlc = pd.read_csv(maindir + insname + item + '/' +folname +'/'+ csvname)
                        ohlc['0'] = pd.to_datetime(ohlc['0'])
                        ohlc['0'] = ohlc['0'].dt.tz_localize(None)
                        instoken = insname + item + type + csvname.replace('.csv', '')+expiry.strftime(dayformat)
                        instrument = {'instrument_token': instoken,
                                      'name': insname,
                                      'strike': strike,
                                      'instrument_type': type,
                                      'segment': segment,
                                      'exchange': exchange,
                                      'tick_size': tick_size,
                                      'expiry': expiry.date()}
                        if instrument not in allins:
                            allins.append(instrument)
                        for timestamp in ohlc['0'].to_list():
                            if timestamp not in tickdict.keys():
                                tickdict[timestamp] = {}
                            try:
                                close = float(ohlc['4'][ohlc['0'] == timestamp])
                                bestbid = float(ohlc['8'][ohlc['0'] == timestamp])
                                bestask = float(ohlc['9'][ohlc['0'] == timestamp])
                                oi = int(ohlc['6'][ohlc['0'] == timestamp])
                                tick = {'depth': {'buy': [{'price': bestbid}], 'sell': [{'price': bestask}]},
                                        'last_price': close,
                                        'oi': oi
                                        }
                                tickdict[timestamp][instoken] = tick
                                # print(tick)
                            except Exception as e:
                                print(e)

lendict = {}
for timestamp in tickdict.keys():
    lendict[timestamp] = len(tickdict[timestamp])

pd.Series(lendict.values()).hist(bins = 50)

import matplotlib.pyplot as plt
tickdict = pickle.load(open('pseudo_ticks','rb'))
x = pd.Series(range(0,len(tickdict)))
y = [len(tickdict[i]) for i in tickdict.keys()]
plt.bar(x,y)



