# import xlwings as xw
from itertools import repeat

import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde

# silent pandas caveats warning
pd.options.mode.chained_assignment = None


#
# book = xw.Book('xlwings.xlsx')
# sheet = book.sheets['temp']
# sheet.clear_contents()

# loc = 'mongo_cache'
# expiries = os.listdir(loc)
# expiry_dates = [datetime.datetime.strptime(expiry.split('_')[1].split('.')[0],'%d%b%Y') for expiry in expiries]
# exp_fname = dict(zip(expiry_dates,expiries))
# expiry_dates.sort()


# loading 5th expiry for sample

# expiry = expiry_dates[4]
# expiry_fname = exp_fname[expiry]
# expiry_file = os.path.join(loc,expiry_fname)
# with open(expiry_file,'rb') as f:
#     expiry_data = pickle.load(f)
#
# timestamps = list(expiry_data.keys())
# timestamp = timestamps[20000]
#
# tsdata = expiry_data[timestamp]
# ltps = tsdata['ltps']
# bidasks = tsdata['bidasks']


def get_spread_breakeven(ltps, spread):
    contract_buy = spread.split('-')[0]
    contract_sell = spread.split('-')[1]
    if contract_sell not in ltps.keys() or contract_buy not in ltps.keys():
        print('Contract not found')
        return None

    ltp_buy = ltps[contract_buy]
    ltp_sell = ltps[contract_sell]

    if 'PE' in contract_buy:
        contract_buy_strike = float(contract_buy.split('PE')[0])
        contract_sell_strike = float(contract_sell.split('PE')[0])
        spread_type = 'PE'
        if contract_sell_strike > contract_buy_strike:
            c_d = 'Credit'
        else:
            c_d = 'Debit'
    elif 'CE' in contract_buy:
        contract_buy_strike = float(contract_buy.split('CE')[0])
        contract_sell_strike = float(contract_sell.split('CE')[0])
        spread_type = 'CE'
        if contract_buy_strike > contract_sell_strike:
            c_d = 'Credit'
        else:
            c_d = 'Debit'

    if c_d == 'Credit':
        premium_received = abs(ltp_sell - ltp_buy)
        width = abs(contract_buy_strike - contract_sell_strike)
        if spread_type == 'CE':
            breakeven = contract_sell_strike + premium_received
            pop = 1 - premium_received / width
        elif spread_type == 'PE':
            breakeven = contract_sell_strike - premium_received
            pop = 1 - premium_received / width
    elif c_d == 'Debit':
        premium_received = abs(ltp_sell - ltp_buy)
        width = abs(contract_buy_strike - contract_sell_strike)
        if spread_type == 'CE':
            breakeven = contract_sell_strike + premium_received
            pop = premium_received / width
        elif spread_type == 'PE':
            breakeven = contract_sell_strike - premium_received
            pop = premium_received / width

    return breakeven, pop


def get_spreads(ltps):
    calls, puts = {}, {}
    for contract in ltps.keys():
        if 'CE' in contract:
            calls[contract] = ltps[contract]
        elif 'PE' in contract:
            puts[contract] = ltps[contract]
    # sory calls and puts by strike
    calls = dict(sorted(calls.items(), key=lambda x: float(x[0].split('CE')[0])))
    puts = dict(sorted(puts.items(), key=lambda x: float(x[0].split('PE')[0])))

    ncalls = len(calls)
    nputs = len(puts)
    ckeys, pkeys = list(calls.keys()), list(puts.keys())
    pkeys.reverse()
    cspreads, pspreads = {}, {}
    for i in range(ncalls):
        for j in range(i + 1, ncalls):
            call_spread = ckeys[j] + '-' + ckeys[i]
            call_breakeven, call_pop = get_spread_breakeven(ltps, call_spread)
            cspreads[call_spread] = {'breakeven': call_breakeven, 'pop': call_pop}
    for i in range(nputs):
        for j in range(i + 1, nputs):
            put_spread = pkeys[j] + '-' + pkeys[i]
            put_breakeven, put_pop = get_spread_breakeven(ltps, put_spread)
            pspreads[put_spread] = {'breakeven': put_breakeven, 'pop': put_pop}
    return pspreads, cspreads


def get_syn_spot(opt_ltps):
    strikes = [int(opt.replace('CE', '').replace('PE', '')) for opt in opt_ltps.keys()]
    strikes = list(set(strikes))
    strikes.sort()
    # remove top and bottom 3
    if len(strikes) > 7:
        strikes = strikes[3:-3]
    spot = pd.Series([strike - opt_ltps[str(strike) + 'PE'] + opt_ltps[str(strike) + 'CE'] for strike in strikes if
                      str(strike) + 'CE' in opt_ltps.keys() and str(strike) + 'PE' in opt_ltps.keys()]).mean()
    return spot


def filter_ltps(ltps, acceptable_percentage):
    filtered_ltps = {}
    spot = get_syn_spot(ltps)
    for contract in ltps.keys():
        if 'CE' in contract:
            strike = float(contract.split('CE')[0])
        if 'PE' in contract:
            strike = float(contract.split('PE')[0])
        if abs(np.log(strike / spot)) < acceptable_percentage:
            filtered_ltps[contract] = ltps[contract]
    return filtered_ltps


def augment_ltps(ltps):
    spot = get_syn_spot(ltps)
    strikes = [int(opt.replace('CE', '').replace('PE', '')) for opt in ltps.keys()]
    strikes = list(set(strikes))
    strikes.sort()
    for strike in strikes:
        if str(strike) + 'CE' not in ltps.keys() and str(strike) + 'PE' in ltps.keys():
            ltps[str(strike) + 'CE'] = spot + ltps[str(strike) + 'PE'] - strike
        if str(strike) + 'PE' not in ltps.keys() and str(strike) + 'CE' in ltps.keys():
            ltps[str(strike) + 'PE'] = strike - spot + ltps[str(strike) + 'CE']
    return ltps


def get_sample(ltps):
    ltps = augment_ltps(ltps)
    put_spreads, call_spreads = get_spreads(ltps)
    put_probabilities = {}
    call_probabilities = {}
    spot = get_syn_spot(ltps)
    for spread in put_spreads.keys():
        if put_spreads[spread]['pop'] < 0:
            continue
        put_probabilities[put_spreads[spread]['breakeven']] = put_spreads[spread]['pop']
    for spread in call_spreads.keys():
        if call_spreads[spread]['pop'] < 0:
            continue
        call_probabilities[call_spreads[spread]['breakeven']] = call_spreads[spread]['pop']

    put_df = pd.DataFrame(list(put_probabilities.items()), columns=['breakeven', 'pop'])
    call_df = pd.DataFrame(list(call_probabilities.items()), columns=['breakeven', 'pop'])
    # put_df.sort_values(by='breakeven',inplace=True)
    # call_df.sort_values(by='breakeven',inplace=True)
    # put_df.reset_index(drop=True,inplace=True)
    # call_df.reset_index(drop=True,inplace=True)
    put_df['pbelow'] = 1 - put_df['pop']
    call_df['pbelow'] = call_df['pop']
    # remove pop from dataframe
    put_df.drop('pop', axis=1, inplace=True)
    call_df.drop('pop', axis=1, inplace=True)
    # join dfs and sort
    df = pd.concat([put_df, call_df], axis=0)
    df.sort_values(by='breakeven', inplace=True)
    df.reset_index(drop=True, inplace=True)
    # normalizing probabilities
    # while pbetween has negative values
    glis = get_longest_increasing_subsequence(list(df['pbelow']))
    # filter df pbelow for longest increasing subsequence
    df = df[df['pbelow'].isin(glis)]
    df.reset_index(drop=True, inplace=True)
    df['pbetween'] = df['pbelow'].shift(-1) - df['pbelow']
    # remove nan
    df['between'] = df['breakeven'].astype(str) + '-' + df['breakeven'].astype(str).shift(-1)
    df.reset_index(drop=True, inplace=True)
    random_sample = []
    for i in range(len(df) - 1):
        lower_bound = df['breakeven'][i]
        upper_bound = df['breakeven'][i + 1]
        pbetween = df['pbetween'][i]
        # get random sample
        if lower_bound <= spot and upper_bound <= spot:
            random_sample.extend(repeat(lower_bound + 0.8 * (upper_bound - lower_bound), int(pbetween * 10000)))
        elif lower_bound >= spot and upper_bound >= spot:
            random_sample.extend(repeat(lower_bound + 0.2 * (upper_bound - lower_bound), int(pbetween * 10000)))
        else:
            mu = (lower_bound + upper_bound) / 2
            sigma = (upper_bound - lower_bound) / 6
            random_sample.extend(np.random.normal(mu, sigma, int(pbetween * 10000)))
    if len(random_sample) > 2:
        # bw = 1.06*np.std(random_sample)
        # types of bw_methods:
        #   - scott: 1.06*sigma
        #   - silverman: 1.06*(4*n**(-1/5)*(min(x) - max(x))**(-1/2))
        #   - normal: 1.06*(min(x) - max(x))
        #   - cunnane: 1.06*(3*n**(-1/5)*(min(x) - max(x))**(-1/2))
        #   - anderson: 1.06*(3*n**(-1/5)*(min(x) - max(x))**(-1/2))
        #   - freedman: 1.06*(3*n**(-4/5)*(min(x) - max(x))**(-1/2))
        #   - box: 1.06*(3*n**(-1/5)*(min(x) - max(x))**(-1/2))
        #   - triang: 1.06*(min(x) - max(x))
        #   - sqrt: 1.06*(min(x) - max(x))
        #   - log: 1.06*(min(x) - max(x))
        #   - log2: 1.06*(min(x) - max(x))

        kde = gaussian_kde(random_sample, bw_method='silverman').resample(size=5000)[0]
    else:
        print('sample too small')
        return None
    return kde


def get_longest_increasing_subsequence(array):
    lis = [1] * len(array)
    for i in range(len(array)):
        _max = -1
        for j in range(0, i + 1):
            if array[i] > array[j]:
                if _max == -1 or _max < lis[j] + 1:
                    _max = lis[j] + 1
        if _max == -1:
            _max = 1
        lis[i] = _max
    result = -1
    index = -1
    for i in range(len(lis)):
        if lis[i] > result:
            result = lis[i]
            index = i

    paths = []
    res = result
    for i in range(index, -1, -1):
        if lis[i] == res:
            paths.append(array[i])
            res -= 1
    return paths[::-1]


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


def get_evs(ltps, bidasks, model_range, trade_range):
    # get sample
    puts = {}
    calls = {}
    for item in ltps.keys():
        if 'CE' in item:
            calls[item] = ltps[item]
        else:
            puts[item] = ltps[item]
    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
    call_spreads = {}
    put_spreads = {}
    try:
        sample = get_sample(filter_ltps(ltps, model_range))
        if not len(sample):
            return {}, {}
    except:
        return {}, {}

    sample.sort()
    df = pd.DataFrame()
    df['spots'] = sample
    spot = get_syn_spot(ltps)
    for option, ltp in ltps.items():
        df[option] = 0
        if abs(np.log(name_to_strike(option) / spot)) > trade_range:
            continue
        if 'CE' in option:
            df[option][df['spots'] > name_to_strike(option)] = df['spots'] - name_to_strike(option)
            df[option] = df[option] - ltp
        else:
            df[option][df['spots'] < name_to_strike(option)] = name_to_strike(option) - df['spots']
            df[option] = df[option] - ltp
        df = df.copy()
    option_evs = {}
    for option in ltps.keys():
        if option in df.columns:
            option_evs[option] = np.mean(df[option]) - bidasks[option] / 2
    if not len(option_evs):
        print('no options in trade range')
        print('length of sample:', len(sample), 'min,max of sample: ', min(sample), max(sample))
        return None
    clist = list(calls.keys())
    plist = list(puts.keys())
    clist.sort()
    plist.sort()
    strikes = set()
    for i in range(0, len(calls) - 2):
        for j in range(i + 1, len(calls) - 1):
            buykey = clist[j]
            sellkey = clist[i]
            spread = buykey + '-' + sellkey
            call_spreads[spread] = option_evs[buykey] - option_evs[sellkey]
    for i in range(0, len(puts) - 2):
        for j in range(i + 1, len(puts) - 1):
            buykey = plist[i]
            sellkey = plist[j]
            spread = buykey + '-' + sellkey
            put_spreads[spread] = option_evs[buykey] - option_evs[sellkey]
    put_spreads = dict(sorted(put_spreads.items(), key=lambda item: item[1]))
    call_spreads = dict(sorted(call_spreads.items(), key=lambda item: item[1]))

    return put_spreads, call_spreads


def get_contract_evs(ltps, bidasks, model_range, trade_range):
    puts = {}
    calls = {}
    for item in ltps.keys():
        if 'CE' in item:
            calls[item] = ltps[item]
        else:
            puts[item] = ltps[item]
    calls = sort_by_strike(calls)
    puts = sort_by_strike(puts)
    call_spreads = {}
    put_spreads = {}
    try:
        sample = get_sample(filter_ltps(ltps, model_range))
        if not len(sample):
            return {}
    except:
        return {}
    sample.sort()
    df = pd.DataFrame()
    df['spots'] = sample
    spot = get_syn_spot(ltps)
    for option, ltp in ltps.items():
        df[option] = 0
        if abs(np.log(name_to_strike(option) / spot)) > trade_range:
            continue
        if 'CE' in option:
            df[option][df['spots'] > name_to_strike(option)] = df['spots'] - name_to_strike(option)
            df[option] = df[option] - ltp
        else:
            df[option][df['spots'] < name_to_strike(option)] = name_to_strike(option) - df['spots']
            df[option] = df[option] - ltp
    option_evs = {}
    for option in ltps.keys():
        if option in df.columns:
            rawev = np.mean(df[option])
            option_evs[option] = rawev - bidasks[option] / 2
            option_evs['-' + option] = - rawev - bidasks[option] / 2
    return option_evs

# model_range = 0.1
# trade_range = 0.04
# t1 = time.time()
# pevs,cevs = get_evs(ltps, bidasks, model_range, trade_range)
# print('time taken:',time.time()-t1)
# import pickle
# import os
# loc = 'mongo_cache'
# file = 'NIFTY_01Oct2020.pkl'
# pb = pickle.load(open(os.path.join(loc,file),'rb'))
# ts = list(pb.keys())[10050]
# ltps = pb[ts]['ltps']
# bidasks = pb[ts]['bidasks']
# model_range = 0.1
# trade_range = 0.04
# sample = get_sample_paretoed(filter_ltps(ltps,model_range))
