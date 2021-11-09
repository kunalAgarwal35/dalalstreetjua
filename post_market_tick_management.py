import os
import shutil
import datetime
import pickle

month = 'aug'
newticks='ticks/'
newoptionticks = 'optionticks/'
oldticks=month+'ticks/'
oldoptionticks = month+'optionticks/'
listnewticks=os.listdir(newticks)
listnewoptionticks = os.listdir(newoptionticks)
iupac="%m%d%Y-%H%M%S-%f"
# expiry = datetime.datetime(2021,6,24)
for file in listnewticks:
    # if datetime.datetime.strptime(file,iupac).date() == expiry.date():
    print(len(os.listdir(newticks)))
    shutil.move(os.path.join(newticks,file),oldticks)

for file in listnewoptionticks:
    # if datetime.datetime.strptime(file,iupac).date() == expiry.date():
    print(len(os.listdir(newoptionticks)))
    shutil.move(os.path.join(newoptionticks,file),oldoptionticks)


tickdir=month+'ticks/'
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




tickdir=month+'optionticks/'
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




