from itertools import repeat

import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde

import function_store2 as fs

pd.options.mode.chained_assignment = None


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


def get_syn_spot(opt_ltps):
    strikes = [int(opt.replace('CE', '').replace('PE', '')) for opt in opt_ltps.keys()]
    strikes = list(set(strikes))
    strikes.sort()
    #remove top and bottom 3
    if len(strikes) > 7:
        strikes = strikes[3:-3]
    spot = pd.Series([strike - opt_ltps[str(strike)+'PE'] + opt_ltps[str(strike)+'CE'] for strike in strikes if str(strike)+'CE' in opt_ltps.keys() and  str(strike)+'PE' in opt_ltps.keys()]).mean()
    return spot


def get_sample(ltps, siz):
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

        kde = gaussian_kde(random_sample, bw_method='silverman').resample(size=siz)[0]
    else:
        print('sample too small')
        return None
    return kde


def get_combo_dist(ltps, timestamp, expiry, vix_range, chain_weight):
    try:
        vix = fs.get_vix_close(timestamp)
        merged_df = fs.merged_df[fs.merged_df['vix_date'] <= timestamp]
        trading_sessions = fs.find_trading_sessions(timestamp, expiry)
        vix_min, vix_max = (1 - vix_range) * vix, (1 + vix_range) * vix
        dist = pd.Series(fs.nifty_distribution_custom(vix_min, vix_max, trading_sessions, merged_df))
        spot = fs.get_syn_spot(ltps)
        dist = spot + (dist * spot / 100)
        sample_size = 10000
        scipy_kde = gaussian_kde(dist).resample(size=int(sample_size * (1 - chain_weight)))[0]
        ocpsample = get_sample(ltps, int(sample_size * (chain_weight)))
        combo_dist = np.concatenate((scipy_kde, ocpsample))
    except:
        return np.array([])
    return combo_dist



def get_evs_from_dist(dist, ltps, bidasks):
    option_evs = {}
    cols = ['spots']
    cols.extend(list(ltps.keys()))
    df = pd.DataFrame(columns=cols)
    df['spots'] = dist
    for option, ltp in ltps.items():
        df[option] = 0
        if 'CE' in option:
            df[option][df['spots'] > fs.name_to_strike(option)] = df['spots'] - fs.name_to_strike(option)
            df[option] = df[option] - ltp
        else:
            df[option][df['spots'] < fs.name_to_strike(option)] = fs.name_to_strike(option) - df['spots']
            df[option] = df[option] - ltp
    option_evs = {}
    for option in ltps.keys():
        if option in df.columns:
            if option not in bidasks.keys(): continue
            rawev = np.mean(df[option])
            option_evs[option] = rawev - bidasks[option] / 2
            option_evs['-' + option] = - rawev - bidasks[option] / 2
    return option_evs

#
#
# import pickle
# import os
# loc = 'mongo_cache_reborn'
# files = os.listdir(loc)
# file = files[3]
# pbdata = pickle.load(open(loc+'/'+file,'rb'))
# timestamplist = list(pbdata.keys())
# timestamplist.sort()
# timestamp = timestamplist[500]
# expiry = datetime.datetime.strptime(file,'NIFTY_%d%b%Y.pkl').date()
# ltps = pbdata[timestamp]['ltps']
# vix_range = 0.1
# chain_weight = 0.5
# sample = get_combo_dist(ltps,timestamp,expiry,vix_range,chain_weight)
# evs = get_evs_from_dist(sample,ltps,pbdata[timestamp]['bidasks'])
# pd.Series(sample).hist(bins = 50)
