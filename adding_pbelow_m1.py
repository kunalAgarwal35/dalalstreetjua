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
from temp_pbelow_test2 import adding_pbelow


loc = 'ev_backtest_data'
toloc = 'ev_backtest_pbelow'
for fname in os.listdir(loc):
    if fname in os.listdir(toloc) or fname.replace('pickle','pbelow_processing') in os.listdir('temp'):
        continue
    else:
        pickle.dump({}, open(os.path.join('temp', fname.replace('pickle', 'pbelow_processing')), 'wb'))
        traindata = adding_pbelow.adding_pbelow({'fname':fname,'from_loc':loc,'to_loc':toloc})
        pickle.dump(traindata,open(os.path.join(toloc,fname),'wb'))
        os.remove(os.path.join('temp', fname.replace('pickle', 'pbelow_processing')))

