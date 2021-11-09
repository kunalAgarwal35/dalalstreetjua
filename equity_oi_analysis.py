import xlwings as xw
import pickle
import os
import datetime
import time
iupac="%m%d%Y-%H%M%S-%f"
dayformat="%y-%m-%d"
dayticks_save_location='dayticks/'
trading_days = os.listdir(dayticks_save_location)

aprins = pickle.load(open('April_kite.instruments','rb'))
mayins = pickle.load(open('May.instruments','rb'))
juneins = pickle.load(open('june.instruments','rb'))
julyins = pickle.load(open('july.instruments','rb'))


# day1 = pickle.load(open(dayticks_save_location+trading_days[0],'rb'))

months = []
dateslist = []
for item in trading_days:
    dateslist.append(datetime.datetime.strptime(item,dayformat))
for item in dateslist:
    mname = item.strftime('%b')
    if mname not in months:
        months.append(mname)

instruments = {months[0]:aprins,months[1]:mayins,months[2]:juneins,months[3]:julyins}


