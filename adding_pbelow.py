#This is to add pbelow fields with different probability models in existing ev_backtest_data dictionaries
import datetime
import pickle
import os
import pandas as pd
import function_store2 as fs
from prob_model import  new_probability_model
import time
#from scipy.stats import norm
#from statistics import NormalDist as norm
import math
import numpy as np


loc = 'ev_backtest_data'
toloc = 'ev_backtest_pbelow'
for fname in os.listdir(loc):
    if fname in os.listdir(toloc) or fname.replace('pickle','pbelow_processing') in os.listdir('temp'):
        continue
    else:
        pickle.dump({}, open(os.path.join('temp', fname.replace('pickle', 'pbelow_processing')), 'wb'))
        expiry = fname[fname.index('_')+1:]
        expiry = expiry[:expiry.index('_')]
        expiry = datetime.datetime.strptime(expiry,'%d%b%Y').date()
        exp_data = pickle.load(open(os.path.join(loc,fname),'rb'))
        traindata = {}
        curr = 0
        total = len(exp_data)
        for timestamp in list(exp_data.keys()):
            curr += 1
            if timestamp not in traindata.keys():
                try:
                    print(curr*100/total)
                    opt_data = exp_data[timestamp]
                    pbelow_x_normalised = new_probability_model(opt_data['ltps'],opt_data['ois'],opt_data['mean'],opt_data['sd'])
                    pbelow_raw_historic = fs.raw_cdf_model(opt_data['spot'],opt_data['vix']*0.8,opt_data['vix']*1.2,timestamp,expiry)
                    traindata[timestamp] = {}
                    old_keys  = list(opt_data.keys())
                    old_keys.append('probability_below_x')
                    old_keys.append('probability_below_x_raw')
                    old_values = list(opt_data.values())
                    old_values.append(pbelow_x_normalised)
                    old_values.append(pbelow_raw_historic)
                    traindata[timestamp].update(zip(old_keys,old_values))
                except:
                    print('Error in ',fname,' ',timestamp)
        pickle.dump(traindata,open(os.path.join(toloc,fname),'wb'))
        os.remove(os.path.join('temp', fname.replace('pickle', 'pbelow_processing')))

