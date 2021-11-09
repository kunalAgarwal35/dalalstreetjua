import os
import datetime
import pandas as pd

dateformat = "%Y-%m-%d"

expiries = os.listdir('2021options')
date_time_expiries = [item.replace('Expiry ','').replace('th','').replace('st','').replace('nd','') for item in expiries]
date_time_expiries = [item[:6] for item in date_time_expiries]
date_time_expiries = [item + ' 2021' for item in date_time_expiries]
name_format = '%d %b %Y'
date_expiries=[]
date_expiries = [datetime.datetime.strptime(item,name_format) for item in date_time_expiries]
for i in range(0,len(expiries)):
    os.rename('2021options/'+expiries[i],'2021options/'+date_expiries[i].strftime(dateformat))
for folder in os.listdir('2021options'):
    for file in os.listdir('2021options/'+folder):
        os.rename('2021options/'+folder+'/'+file,'2021options/'+folder+'/'+file.replace('NIFTY','').replace('WK','').replace(' ',''))

for folder in os.listdir('2021options'):
    if 'rec' not in os.listdir('2021options/'+folder):
        os.makedirs('2021options/'+folder+'/'+'rec')
    for file in os.listdir('2021options/'+folder):
        if '.csv' in file:
            df=pd.read_csv('2021options/'+folder+'/'+file,header=None)
            dates = [datetime.datetime.strptime(item, '%Y/%m/%d') for item in df[1].to_list()]
            times = [datetime.datetime.strptime(item, '%H:%M') for item in df[2].to_list()]
            datetimes = [datetime.datetime.combine(dates[i],times[i].time()) for i in range(0,len(dates)-1)]
            ndf=pd.DataFrame()
            ndf['date'] = datetimes
            ndf['open'] = df[3]
            ndf['high'] = df[4]
            ndf['low'] = df[5]
            ndf['close'] = df[6]
            ndf['volume'] = df[7]
            ndf['oi'] = df[8]
            ndf.to_csv('2021options/'+folder+'/'+'rec/'+file,index=False)
