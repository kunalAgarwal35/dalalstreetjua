import os
import pickle
import numpy as np
import datetime
import pandas as pd
import time



tickdir='julyticks/'
iupac="%m%d%Y-%H%M%S-%f"
dayformat="%y-%m-%d"
dayticks_save_location='dayticks/'
ticklist = os.listdir(tickdir)
dates_list = [datetime.datetime.strptime(date, iupac) for date in ticklist]
start_time=datetime.time(9,15,10)
end_time=datetime.time(15,30,00)
trading_days=[]
for item in dates_list:
    if start_time < item.time() < end_time:
        if item.date() not in trading_days:
            trading_days.append(item.date())
saved_dates=[datetime.datetime.strptime(date,dayformat) for date in os.listdir(dayticks_save_location)]
saved_dates=[i.date() for i in saved_dates]
for savedate in trading_days:
    if savedate not in saved_dates:
        print(savedate)
        tickdict={}
        for item in dates_list:
            if item.date() == savedate and start_time < item.time() < end_time:
                print(item)
                try:
                    with open(tickdir + item.strftime(iupac), 'rb') as handle:
                        tick = pickle.load(handle)
                    handle.close()
                    tickdict[item]=tick
                except:
                    continue
        pickle.dump(tickdict, open(dayticks_save_location + savedate.strftime(dayformat), "wb"))




tickdir='julyoptionticks/'
iupac="%m%d%Y-%H%M%S-%f"
dayformat="%y-%m-%d"
dayticks_save_location='dayoptionticks/'
ticklist = os.listdir(tickdir)
dates_list = [datetime.datetime.strptime(date, iupac) for date in ticklist]
start_time=datetime.time(9,15,10)
end_time=datetime.time(15,30,00)
trading_days=[]
for item in dates_list:
    if start_time < item.time() < end_time:
        if item.date() not in trading_days:
            trading_days.append(item.date())
saved_dates=[datetime.datetime.strptime(date,dayformat) for date in os.listdir(dayticks_save_location)]
saved_dates=[i.date() for i in saved_dates]
for savedate in trading_days:
    if savedate not in saved_dates:
        print(savedate)
        tickdict={}
        for item in dates_list:
            if item.date() == savedate and start_time < item.time() < end_time:
                print(item)
                try:
                    with open(tickdir + item.strftime(iupac), 'rb') as handle:
                        tick = pickle.load(handle)
                    handle.close()
                    tickdict[item]=tick
                except:
                    continue
        pickle.dump(tickdict, open(dayticks_save_location + savedate.strftime(dayformat), "wb"))
