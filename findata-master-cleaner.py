import pandas as pd
import numpy as np
import os
import datetime
import dateutil

allopts = os.path.join('findata-master','BANKNIFTY','options')
expiries = os.listdir(allopts)
dateformat = '%d.%m.%y %H:%M:%S'
columnnames = ['timestamp','open','high','low','close','volume','oi','vwap','bid','ask']
for expiry in expiries:
    contracts = os.listdir(os.path.join(allopts,expiry))
    for contract in contracts:
        try:
            # print(os.path.join(allopts,expiry,contract))
            file = pd.read_csv(os.path.join(allopts,expiry,contract))
            dtcol = file['0']
            dtcol = [datetime.datetime.strptime(i,dateformat).replace(second=0) for i in dtcol]
            file['0'] = dtcol
            file.columns = columnnames[:len(file.columns)]
            oi = file['oi']
            oi_corr = []
            for item in oi:
                if len(oi_corr) and not item:
                    oi_corr.append(oi_corr[len(oi_corr)-1])
                else:
                    oi_corr.append(item)
            file['oi'] = oi_corr
            file.to_csv(os.path.join(allopts,expiry,contract),index=False)
        except Exception as e:
            print(e,os.path.join(allopts,expiry,contract))