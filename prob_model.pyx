import time
from scipy.stats import norm
#from statistics import NormalDist as norm
import math
import numpy as np
import pickle
import pandas as pd

#from Cython.Compiler.Options import get_directive_defaults
#directive_defaults = get_directive_defaults()
#directive_defaults['linetrace'] = True
#directive_defaults['binding'] = True

cdef extern from "norm_cdf.c":
    double norm_cdf(double val)

def round_down(x):
    a = 0.01
    return math.floor(x / a) * a


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))


def sort_by_strike(calls):
    _ = {}
    __ = {}
    for contract in calls.keys():
        _[name_to_strike(contract)] = calls[contract]
        __[name_to_strike(contract)] = contract
    ret = {}
    _ = dict(sorted(_.items()))
    for key in _.keys():
        ret[__[key]] = _[key]
    return ret

#@profile
def new_probability_model(opt_ltps, opt_ois, mean, sd):
    t1 = time.time()
    data = np.arange(round_down(-10), round_down(10), 0.01)
    cdfd = norm.cdf(data, loc=mean, scale=sd)
    puts = {}
    calls = {}
    for item in opt_ltps.keys():
        if 'CE' in item:
            calls[item] = opt_ltps[item]
        elif 'PE' in item:
            puts[item] = opt_ltps[item]

    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)

    _ = list(calls.keys())
    ma = max((name_to_strike(_[0]),name_to_strike(_[len(_)-1])))
    mi = min((name_to_strike(_[0]),name_to_strike(_[len(_)-1])))
    pseudo_spot = (ma+mi)/2
    _ = list(puts.keys())
    ma = max(ma,max((name_to_strike(_[0]) , name_to_strike(_[len(_) - 1]))))
    mi = min(mi, min((name_to_strike(_[0]) , name_to_strike(_[len(_) - 1]))))

    xs = list(np.arange(mi,ma,10))
    prob_below = {}
    oik = {}
    oiv = {}
    ##test_new_idea = []
    distnow = norm(loc=mean, scale=sd)
    for i in range(0, len(calls.keys()) - 2):
        for j in range(i + 1, len(calls.keys()) - 1):
            keyi = list(calls.keys())[i]
            keyj = list(calls.keys())[j]
            credit_collected = calls[keyi] - calls[keyj]
            width = int(keyj.replace('CE', '')) - int(keyi.replace('CE', ''))
            pop = 1 - credit_collected / width
            be = int(keyi.replace('CE', '')) + credit_collected
            ind = len(data) - (cdfd > pop).sum() - 1
            pct = data[ind]
            center = be - be * (pct / 100)
            for x in xs:
                if x not in oik.keys():
                    oik[x] = []
                    oiv[x] = []
                x_pct = 100 * np.log(x / center)
                probability = norm_cdf((x_pct - mean)/sd) ##distnow.cdf(x_pct)
                oik[x].append(min(opt_ois[keyi], opt_ois[keyj]))
                oiv[x].append(probability)
                ##test_new_idea.append(x_pct)
            ##test_new_prob = (distnow.cdf(test_new_idea))
    ##test_new_idea = []
    for i in range(0, len(puts.keys()) - 2):
        for j in range(i + 1, len(puts.keys()) - 1):
            keyi = list(puts.keys())[i]
            keyj = list(puts.keys())[j]
            credit_collected = puts[keyj] - puts[keyi]
            width = int(keyj.replace('PE', '')) - int(keyi.replace('PE', ''))
            pop = 1 - credit_collected / width
            be = int(keyj.replace('PE', '')) - credit_collected
            ind = len(data) - (cdfd > (1 - pop)).sum() - 1
            pct = data[ind]
            center = be - be * (pct / 100)
            for x in xs:
                if x not in oik.keys():
                    oik[x] = []
                    oiv[x] = []
                x_pct = 100 * np.log(x / center)
                probability = norm_cdf((x_pct - mean)/sd) ##distnow.cdf(x_pct)
                oik[x].append(min(opt_ois[keyi], opt_ois[keyj]))
                oiv[x].append(probability)
                ##test_new_idea.append(x_pct)
           ##test_new_prob = (distnow.cdf(test_new_idea))
    for x in xs:
        cum_probability = (pd.Series(oik[x]) * pd.Series(oiv[x])).sum()
        cum_probability = 100 * cum_probability / (pd.Series(oik[x]).sum())
        prob_below[x] = cum_probability
    prob_below = dict(sorted(prob_below.items()))
    print(time.time() - t1, 'probability_below_cython')
    
    return prob_below
# =============================================================================
# 
# spd = pickle.load(open('sample_data.pkl','rb'))
# timestamp = list(spd.keys())[6]
# opt_ltps = spd[timestamp]['opt_ltps']
# opt_ois = spd[timestamp]['opt_oi']
# mean = spd[timestamp]['mean']
# sd = spd[timestamp]['sd']
# 
# new_probability_model(opt_ltps, opt_ois,mean,sd)
# =============================================================================
