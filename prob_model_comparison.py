import os

import pandas as pd
import pickle
import numpy as np
import function_store2 as fs
import xlwings as xw
import time
import statistics
from scipy.stats import norm
import math
import datetime
import opt_chain_probmodel as ocp
import cProfile

file_path = 'xlwings.xlsx'
book = xw.Book(file_path)
s = book.sheets['pbelowtest']


def load_pbelow_pickle():
    print('Reading pickle')
    try:
        pb = pickle.load(open('ronit_traindata.pkl','rb'))
    except:
        time.sleep(0.1)
        pb = load_pbelow_pickle()
    return pb

# pb = load_pbelow_pickle()

def expiry_probability_monte(data):
    pct_change = 100*np.log(data['index_expiry_price']/data['spot'])
    pbelow = ocp.get_sample(data['ltps'])
    rr = np.arange(int(min(pbelow)),int(max(pbelow)),5)
    pbelow = pd.Series(pbelow)
    rr = pd.Series(rr)
    # _ = [len([i for i in pbelow if i < rr[j]])*100/len(pbelow) for j in range(0,len(rr))]
    _ = [100*pbelow[pbelow < j].count()/pbelow.count() for j in rr]
    pbelow = dict(zip(rr,_))
    items = list(pbelow.items())

    below_edge = 0
    above_edge = 0
    exp = data['index_expiry_price']
    last_item = len(pbelow.items())-1
    prob_event = 0
    if exp < items[0][0]:
        below_edge = 1
        prob_event = items[0][1]
    elif exp > items[last_item][0]:
        above_edge = 1
        prob_event = 100
    else:
        for i in range(0,last_item+1):
            if exp < items[i+1][0]:
                prob_event = items[i+1][1]
                break

    return prob_event,pct_change

def expiry_probability_range(data, distnow):
    pct_change = 100*np.log(data['index_expiry_price']/data['spot'])
    pbelow = data['probability_below_x']
    items = list(pbelow.items())
    below_edge = 0
    above_edge = 0
    exp = data['index_expiry_price']
    last_item = len(pbelow.items())-1
    prob_event = 0
    if exp < items[0][0]:
        below_edge = 1
        prob_event = items[0][1]
    elif exp > items[last_item][0]:
        above_edge = 1
        prob_event = 100
    else:
        for i in range(0,last_item+1):
            if exp < items[i+1][0]:
                prob_event = items[i+1][1]
                break
    prob_event_normal = 100*distnow.cdf(pct_change)
    return prob_event,pct_change, prob_event_normal
def normal_distribution_cdfable(mean,sd):
    data = np.arange(fs.round_down(-30), fs.round_down(30), 0.01)
    pdf = norm.pdf(data, loc=mean, scale=sd)
    cdf = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    distnow = norm(loc=mean, scale=sd)
    # probability = distnow.cdf(pct_change)
    return distnow
def print_distribution(timestamp,data):
    pbelowdict = data['probability_below_x']
    trading_sessions = fs.find_trading_sessions(timestamp,data['expiry'])
    dist = pd.DataFrame()
    dist['spot'] = list(pbelowdict.keys())
    dist['pbelow'] = list(pbelowdict.values())
    dist['pbelow_shift'] = dist['pbelow'].shift(1)
    dist['pbelow_shift2'] = dist['pbelow_shift'].fillna(0)
    dist['dist'] = dist['pbelow'] - dist['pbelow_shift']
    sumdist = (dist['pbelow'] - dist['pbelow_shift2']).sum()
    dist = dist[['spot','dist']]
    exp = data['index_expiry_price']
    cur = data['spot']
    current, expiry = [],[]
    for i in dist['spot']:
        if i > cur:
            cur = 1000000
            current.append(1)
        else:
            current.append(0)
        if i > exp:
            expiry.append(1)
            exp = 1000000
        else:
            expiry.append(0)
    dist['expiry'] = expiry
    dist['current'] = current
    s.range('W1:AA2000').clear_contents()
    s.range('W1').value = dist
    s.range('T1').value = 'Candles to Expiry'
    s.range('U1').value = trading_sessions
    s.range('V1').value = 'Mean'
    s.range('V2').value = data['mean']
    s.range('V3').value = 'SD'
    s.range('V4').value = data['sd']
    # s.range('V5').value = 'Sum Dist'
    # s.range('V6').value = sumdist
    if not 90 < sumdist < 100:
        time.sleep(2)
    return
df = pd.DataFrame()
df['pct'] = np.arange(1,100,1)
df['freq'] = 0
df['occ'] = 0
df['freq_norm'] = 0
df['occ_norm'] = 0
s.clear_contents()
# distnow = normal_distribution_cdfable(mean=0,sd=1)
eventcount = 0
sdate = datetime.datetime(2015,1,1)
todate = datetime.datetime(2021,12,30)

pbelow_dir = 'ev_backtest_data'
for file in os.listdir(pbelow_dir):
    print(file)
    pb = pickle.load(open(os.path.join(pbelow_dir,file),'rb'))
    expiry_date = datetime.datetime.strptime(file.split('_')[1],'%d%b%Y').date()
    print((os.listdir(pbelow_dir).index(file)*100/len(os.listdir(pbelow_dir))))
    for timestamp in pb.keys():
        try:
            if sdate < timestamp <todate:
                data = pb[timestamp]
                if len(data['ltps']) < 25:
                    continue
                data['expiry'] = expiry_date
                prob_event,pct_change = expiry_probability_monte(data)
                # df = df.append({'timestamp':timestamp,'prob_event':prob_event,'pct_change':pct_change},ignore_index=True)
                freq = []
                eventcount += 1
                for i in range(0,len(df)):
                    if df['pct'][i] > prob_event:
                        freq.append(int(df['freq'][i])+1)
                    else:
                        freq.append(int(df['freq'][i]))
                df['freq'] = freq
                # df['occ'] = df['freq']*100/max(df['freq'])
                df['occ'] = df['freq'] * 100 / eventcount
                # if timestamp.minute == 0 and timestamp.second == 0:
                s.range('A1').value = df
                # print_distribution(timestamp,data)
                print(timestamp)
                opt_ltps = data['ltps']
                opt_ois = data['ois']
                spot = data['spot']
        except Exception as e:
            print(e)
            # print(fs.spread_evs_simple(opt_ltps=opt_ltps,opt_ois=opt_ois,mean=mean,sd=sd,l=l,u=u,spot=spot))
