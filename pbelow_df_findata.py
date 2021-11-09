import pandas as pd
import numpy as np
import function_store as fs
import os
import pickle
import datetime



index = 'NIFTY'
vix_range_percent = 0.2
path = os.path.join('findata-master',index,'options')

nifty = pickle.load(open('nifty_historical.pkl','rb'))
vix = pickle.load(open('vix_historical.pkl','rb'))
vix = vix.loc[vix['date'].isin(nifty['date'])]
nifty = nifty.loc[nifty['date'].isin(vix['date'])]
dateformat = '%Y-%m-%d %H:%M:%S'
prob_range = 0.06
start_date = datetime.datetime(2018,8,3)
contracts = {}
if 'ronit_traindata.pkl' in os.listdir():
    ronit_traindata = pickle.load(open('ronit_traindata.pkl','rb'))
else:
    ronit_traindata = {}
try:
    start_date = list(ronit_traindata.keys())[len(ronit_traindata)-1]
except:
    pass
for timestamp in nifty['date'][nifty['date']>start_date]:
    # timestamp = timestamp.to_pydatetime()
    try:
        # if timestamp in ronit_traindata.keys():
        #     continue
        print(timestamp)
        expiry = fs.find_expiry(timestamp,path)
        vixnow = float(vix['close'][vix['date'] == timestamp])
        mean,sd = fs.get_stats_from_vix(timestamp,vix_range_percent,vixnow,expiry)
        if expiry not in contracts.keys():
            pickle.dump(ronit_traindata, open('ronit_traindata.pkl', 'wb'))
            contracts[expiry] = {}
            conpath = os.path.join(path,expiry.strftime('%Y-%m-%d'))
            for file in os.listdir(conpath):
                contracts[expiry][file.replace('.csv','')] = pd.read_csv(os.path.join(conpath,file))
            for key in list(contracts.keys()):
                if key != expiry:
                    contracts.pop(key)
            poplist = []
            for contract in contracts[expiry].keys():
                if 'timestamp' not in contracts[expiry][contract].columns:
                    poplist.append(contract)
            for contract in poplist:
                contracts[expiry].pop(contract)
        opt_ltps,opt_ois = fs.update_ltp_oi_findata(timestamp,contracts[expiry],dateformat)
        if len(opt_ltps):
            spot = float(nifty['close'][nifty['date']==timestamp])
            args = [opt_ltps,opt_ois,(1-prob_range)*spot,(1+prob_range)*spot,mean,sd]
            # pb = fs.new_probability_model(opt_ltps, opt_ois, mean, sd)
            put_spreads,call_spreads,pb = fs.spread_evs_custom_new_model_with_pbelow(*args)
            expiry_spot = float(nifty['close'][nifty['date']==datetime.datetime(expiry.year,expiry.month,expiry.day,15,25,0)])
            put_spreads_result, call_spreads_result = fs.find_spread_results(put_spreads,call_spreads,expiry_spot,opt_ltps)
            ronit_traindata[timestamp] = {'probability_below_x':pb,'put_spread_ev':put_spreads,'put_spread_result':put_spreads_result,
                                          'call_spread_ev':call_spreads,'call_spread_result':call_spreads_result,
                                          'vix':vixnow,'spot':spot,'index_expiry_price':expiry_spot,'last_price':opt_ltps,'oi':opt_ois,
                                          'historical_mean':mean,'historical_sd':sd,'expiry':expiry}

    except Exception as e:
        print(e)
