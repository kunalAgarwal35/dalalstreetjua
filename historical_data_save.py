import zd
import os
import datetime
import dateutil
import pandas as pd
import time


allins = zd.kite.instruments()
dayformat = '%y-%m-%d'
save_tokens = {}
curr_ins = {}
for item in allins:
    curr_ins[item['instrument_token']] = item
    if item['exchange'] == 'NFO':
        save_tokens[item['instrument_token']] = item

names = []
expiries = []
types = []
for key in save_tokens.keys():
    if save_tokens[key]['name'] not in names:
        names.append(save_tokens[key]['name'])
    if save_tokens[key]['instrument_type'] not in types:
        types.append(save_tokens[key]['instrument_type'])
    if save_tokens[key]['expiry'] not in types:
        expiries.append(save_tokens[key]['expiry'])


expiry_str = [i.strftime(dayformat) for i in expiries]

opt_his = 'kitehistorical/options/'
fut_his = 'kitehistorical/futures/'

for name in names:
    if name not in os.listdir(opt_his):
        os.mkdir(opt_his+name)
    if name not in os.listdir(fut_his):
        os.mkdir(fut_his+name)


one_year = datetime.timedelta(days=365)
one_day = datetime.timedelta(days=1)
now = datetime.datetime.today()+one_day
for name in names:
    print(name)
    for key,ins in save_tokens.items():
        if ins['name'] == name:
            try:
                df = zd.get_historical(instrument_token=key,fdate = now-one_year,tdate=now,interv='minute',oi=True)
            except Exception as e:
                print(e)
                if 'Incorrect' in str(e) and 'api_key' in str(e):
                    import zd
                continue
            if len(df):
                instype = ins['instrument_type']
                expiry = ins['expiry'].strftime(dayformat)
                if instype in ['CE','PE']:
                    if expiry not in os.listdir(opt_his+ins['name']):
                        os.mkdir(opt_his+ins['name']+'/'+expiry)
                    fname = expiry+'/'+str(int(ins['strike']))+instype
                    fname = opt_his+ins['name']+'/'+fname+'.csv'
                    df.to_csv(fname,index=False)
                    print(fname)
                else:
                    fname = fut_his+ins['name']+'/'+expiry+'.csv'
                    df.to_csv(fname, index=False)
                    print(fname)


