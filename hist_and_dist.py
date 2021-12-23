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



def get_spread_breakeven_tripple(ltps, spread,bidasks):
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
        premium_received_1 = abs(ltp_sell - bidasks[contract_sell]/2 - ltp_buy + bidasks[contract_buy]/2)
        premium_received_2 = abs(ltp_sell + bidasks[contract_sell]/2 - ltp_buy - bidasks[contract_buy]/2)
        width = abs(contract_buy_strike - contract_sell_strike)
        if spread_type == 'CE':
            breakeven = contract_sell_strike + premium_received
            breakeven_1 = contract_sell_strike + premium_received_1
            breakeven_2 = contract_sell_strike + premium_received_2

            pop = 1 - premium_received / width
            pop_1 = 1 - premium_received_1 / width
            pop_2 = 1 - premium_received_2 / width
        elif spread_type == 'PE':
            breakeven = contract_sell_strike - premium_received
            breakeven_1 = contract_sell_strike - premium_received_1
            breakeven_2 = contract_sell_strike - premium_received_2

            pop = 1 - premium_received / width
            pop_1 = 1 - premium_received_1 / width
            pop_2 = 1 - premium_received_2 / width
    elif c_d == 'Debit':
        premium_received = abs(ltp_sell - ltp_buy)
        premium_received_1 = abs(ltp_sell - bidasks[contract_sell]/2 - ltp_buy + bidasks[contract_buy]/2)
        premium_received_2 = abs(ltp_sell + bidasks[contract_sell]/2 - ltp_buy - bidasks[contract_buy]/2)
        width = abs(contract_buy_strike - contract_sell_strike)
        if spread_type == 'CE':
            breakeven = contract_sell_strike + premium_received
            breakeven_1 = contract_sell_strike + premium_received_1
            breakeven_2 = contract_sell_strike + premium_received_2

            pop = premium_received / width
            pop_1 = premium_received_1 / width
            pop_2 = premium_received_2 / width
        elif spread_type == 'PE':
            breakeven = contract_sell_strike - premium_received
            breakeven_1 = contract_sell_strike - premium_received_1
            breakeven_2 = contract_sell_strike - premium_received_2

            pop = premium_received / width
            pop_1 = premium_received_1 / width
            pop_2 = premium_received_2 / width

    return breakeven,breakeven_1,breakeven_2,pop,pop_1,pop_2


def get_spreads(ltps, bidasks):
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
            # call_breakeven, call_pop = get_spread_breakeven(ltps, call_spread)
            # cspreads[call_spread] = {'breakeven': call_breakeven, 'pop': call_pop}
            call_breakeven, call_breakeven_1, call_breakeven_2, call_pop, call_pop_1, call_pop_2 = get_spread_breakeven_tripple(ltps, call_spread,bidasks)
            cspreads[call_spread] = {'breakeven': call_breakeven, 'breakeven_1': call_breakeven_1, 'breakeven_2': call_breakeven_2, 'pop': call_pop, 'pop_1': call_pop_1, 'pop_2': call_pop_2}
    for i in range(nputs):
        for j in range(i + 1, nputs):
            put_spread = pkeys[j] + '-' + pkeys[i]
            # put_breakeven, put_pop = get_spread_breakeven(ltps, put_spread)
            # pspreads[put_spread] = {'breakeven': put_breakeven, 'pop': put_pop}
            put_breakeven, put_breakeven_1, put_breakeven_2, put_pop, put_pop_1, put_pop_2 = get_spread_breakeven_tripple(ltps, put_spread,bidasks)
            pspreads[put_spread] = {'breakeven': put_breakeven, 'breakeven_1': put_breakeven_1, 'breakeven_2': put_breakeven_2, 'pop': put_pop, 'pop_1': put_pop_1, 'pop_2': put_pop_2}
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


def get_sample(ltps, siz, bidasks):
    put_spreads, call_spreads = get_spreads(ltps, bidasks)
    put_probabilities = {}
    call_probabilities = {}
    spot = get_syn_spot(ltps)
    for spread in put_spreads.keys():
        if put_spreads[spread]['pop'] > 0:
            put_probabilities[put_spreads[spread]['breakeven']] = put_spreads[spread]['pop']
        if put_spreads[spread]['pop_1'] > 0:
            put_probabilities[put_spreads[spread]['breakeven_1']] = put_spreads[spread]['pop_1']
        if put_spreads[spread]['pop_2'] > 0:
            put_probabilities[put_spreads[spread]['breakeven_2']] = put_spreads[spread]['pop_2']
    for spread in call_spreads.keys():
        if call_spreads[spread]['pop'] > 0:
            call_probabilities[call_spreads[spread]['breakeven']] = call_spreads[spread]['pop']
        if call_spreads[spread]['pop_1'] > 0:
            call_probabilities[call_spreads[spread]['breakeven_1']] = call_spreads[spread]['pop_1']
        if call_spreads[spread]['pop_2'] > 0:
            call_probabilities[call_spreads[spread]['breakeven_2']] = call_spreads[spread]['pop_2']

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


def get_combo_dist(ltps, timestamp, expiry, vix_range, chain_weight,bidasks):
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
        ocpsample = get_sample(ltps, int(sample_size * (chain_weight)),bidasks)
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

def get_sample_normal_mutation(ltps, siz, bidasks):
    put_spreads, call_spreads = get_spreads(ltps, bidasks)
    put_probabilities = {}
    call_probabilities = {}
    spot = get_syn_spot(ltps)
    for spread in put_spreads.keys():
        if put_spreads[spread]['pop'] > 0:
            put_probabilities[put_spreads[spread]['breakeven']] = put_spreads[spread]['pop']
        if put_spreads[spread]['pop_1'] > 0:
            put_probabilities[put_spreads[spread]['breakeven_1']] = put_spreads[spread]['pop_1']
        if put_spreads[spread]['pop_2'] > 0:
            put_probabilities[put_spreads[spread]['breakeven_2']] = put_spreads[spread]['pop_2']
    for spread in call_spreads.keys():
        if call_spreads[spread]['pop'] > 0:
            call_probabilities[call_spreads[spread]['breakeven']] = call_spreads[spread]['pop']
        if call_spreads[spread]['pop_1'] > 0:
            call_probabilities[call_spreads[spread]['breakeven_1']] = call_spreads[spread]['pop_1']
        if call_spreads[spread]['pop_2'] > 0:
            call_probabilities[call_spreads[spread]['breakeven_2']] = call_spreads[spread]['pop_2']

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
    for i in range(len(df)):
        pbelow_value  = df.loc[i, 'pbelow']
        # count number of timees pbelow_value occurs
        pbelow_count = df[df['pbelow'] == pbelow_value].shape[0]
        while (pbelow_count > 1):
            pbelow_value -= 0.0001
            df.loc[i, 'pbelow'] = pbelow_value
            pbelow_count = df[df['pbelow'] == pbelow_value].shape[0]
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
    # get normal sample between 0.8* spot to 1.2* spot
    sd = (df['breakeven'].max() - df['breakeven'].min()) / 6
    nms = np.random.normal(loc=spot, scale=sd, size=siz*10000)
    # pd.Series(nms).hist(bins = 100)
    for i in range(len(df)):
        l,u = float(df['breakeven'][i]), float(df['breakeven'].shift(-1)[i])
        try:
            pb = int(df['pbetween'][i] * siz)
        except:
            continue
        if pb <= 0:
            continue
        # count elements in nms between l and u
        cnt = np.sum(np.logical_and(nms > l, nms < u))
        # if there are more elements than pb
        subsample  = nms[np.logical_and(nms > l, nms < u)]
        # taking pb number of elements from subsample randomly
        random_sample.extend(np.random.choice(subsample, pb, replace=False))


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

# def get_sample_raw(ltps):
#     put_spreads, call_spreads = get_spreads(ltps)
#     put_probabilities = {}
#     call_probabilities = {}
#     spot = get_syn_spot(ltps)
#     for spread in put_spreads.keys():
#         if put_spreads[spread]['pop'] < 0:
#             continue
#         put_probabilities[put_spreads[spread]['breakeven']] = put_spreads[spread]['pop']
#     for spread in call_spreads.keys():
#         if call_spreads[spread]['pop'] < 0:
#             continue
#         call_probabilities[call_spreads[spread]['breakeven']] = call_spreads[spread]['pop']
#
#     put_df = pd.DataFrame(list(put_probabilities.items()), columns=['breakeven', 'pop'])
#     call_df = pd.DataFrame(list(call_probabilities.items()), columns=['breakeven', 'pop'])
#     # put_df.sort_values(by='breakeven',inplace=True)
#     # call_df.sort_values(by='breakeven',inplace=True)
#     # put_df.reset_index(drop=True,inplace=True)
#     # call_df.reset_index(drop=True,inplace=True)
#     put_df['pbelow'] = 1 - put_df['pop']
#     call_df['pbelow'] = call_df['pop']
#     # remove pop from dataframe
#     put_df.drop('pop', axis=1, inplace=True)
#     call_df.drop('pop', axis=1, inplace=True)
#     # join dfs and sort
#     df = pd.concat([put_df, call_df], axis=0)
#     df.sort_values(by='breakeven', inplace=True)
#     df.reset_index(drop=True, inplace=True)
#     # normalizing probabilities
#     # while pbetween has negative values
#     glis = get_longest_increasing_subsequence(list(df['pbelow']))
#     # filter df pbelow for longest increasing subsequence
#     df = df[df['pbelow'].isin(glis)]
#     df.reset_index(drop=True, inplace=True)
#     df['pbetween'] = df['pbelow'].shift(-1) - df['pbelow']
#     # remove nan
#     df['between'] = df['breakeven'].astype(str) + '-' + df['breakeven'].astype(str).shift(-1)
#     df.reset_index(drop=True, inplace=True)
#     random_sample = []
#     for i in range(len(df) - 1):
#         lower_bound = df['breakeven'][i]
#         upper_bound = df['breakeven'][i + 1]
#         pbetween = df['pbetween'][i]
#         # get random sample
#         if lower_bound <= spot and upper_bound <= spot:
#             random_sample.extend(repeat(lower_bound + 0.8 * (upper_bound - lower_bound), int(pbetween * 10000)))
#         elif lower_bound >= spot and upper_bound >= spot:
#             random_sample.extend(repeat(lower_bound + 0.2 * (upper_bound - lower_bound), int(pbetween * 10000)))
#         else:
#             mu = (lower_bound + upper_bound) / 2
#             sigma = (upper_bound - lower_bound) / 6
#             random_sample.extend(np.random.normal(mu, sigma, int(pbetween * 10000)))
#
#     return random_sample
#
#
#
# ltps = {"11200PE":7.75,"11300PE":10.425,"11400PE":14.225000000000001,"11450PE":16.25,"11500PE":19.225,"11550PE":22.5,"11600PE":27.6,"11650PE":33.125,"11700PE":41.35,"11750PE":49.599999999999994,"11800PE":61.15,"11850PE":73.65,"11900PE":89.5,"11950PE":106.0,"12000PE":126.425,"12050PE":150.825,"12100PE":177.925,"12150PE":207.375,"12200PE":241.075,"12250PE":279.475,"12300PE":317.175,"12350PE":360.875,"12400PE":404.825,"12500PE":495.475,"12600PE":594.2,"12700PE":690.675,"12800PE":793.0250000000001,"12900PE":888.175,"13000PE":988.3,"13200PE":1185.05,"13300PE":1283.525,"13500PE":1484.775,"13700PE":1677.5749999999998,"13800PE":1783.5749999999998,"10350CE":1651.15,"10400CE":1602.425,"10500CE":1504.375,"10700CE":1307.275,"10800CE":1208.65,"10900CE":1110.675,"11000CE":1010.0250000000001,"11200CE":811.975,"11300CE":717.075,"11450CE":573.875,"11500CE":524.65,"11550CE":482.57500000000005,"11600CE":433.45000000000005,"11650CE":391.1,"11700CE":349.8,"11750CE":306.0,"11800CE":269.2,"11900CE":197.925,"11950CE":165.425,"12000CE":135.3,"12050CE":110.225,"12100CE":87.225,"12150CE":68.775,"12200CE":52.05,"12250CE":39.2,"12300CE":28.35,"12350CE":20.0,"12400CE":14.825,"12450CE":11.425,"12500CE":8.425,"12550CE":6.875,"12600CE":5.65,"12650CE":4.8,"12700CE":4.35,"12800CE":3.5999999999999996}
# bidasks = {"11200PE":0.09999999999999964,"11300PE":0.15000000000000036,"11400PE":0.15000000000000036,"11450PE":0.20000000000000284,"11500PE":0.15000000000000213,"11550PE":0.20000000000000284,"11600PE":0.09999999999999787,"11650PE":0.25,"11700PE":0.20000000000000284,"11750PE":0.6000000000000014,"11800PE":0.20000000000000284,"11850PE":0.6000000000000085,"11900PE":0.30000000000001137,"11950PE":0.5,"12000PE":0.25,"12050PE":0.6500000000000057,"12100PE":0.25,"12150PE":5.350000000000023,"12200PE":0.8499999999999943,"12250PE":5.349999999999966,"12300PE":2.6499999999999773,"12350PE":14.350000000000023,"12400PE":2.25,"12500PE":4.75,"12600PE":6.199999999999932,"12700PE":10.75,"12800PE":14.149999999999977,"12900PE":41.14999999999998,"13000PE":5.399999999999977,"13200PE":18.59999999999991,"13300PE":15.549999999999955,"13500PE":6.149999999999864,"13700PE":27.049999999999955,"13800PE":11.450000000000045,"10350CE":10.899999999999864,"10400CE":8.349999999999909,"10500CE":5.349999999999909,"10700CE":17.350000000000136,"10800CE":18.299999999999955,"10900CE":21.25,"11000CE":3.8500000000000227,"11200CE":7.0499999999999545,"11300CE":9.949999999999932,"11450CE":26.25,"11500CE":2.7999999999999545,"11550CE":17.05000000000001,"11600CE":4.199999999999989,"11650CE":6.5,"11700CE":4.899999999999977,"11750CE":8.5,"11800CE":1.5,"11900CE":1.049999999999983,"11950CE":3.049999999999983,"12000CE":0.5999999999999943,"12050CE":0.75,"12100CE":0.25,"12150CE":0.45000000000000284,"12200CE":0.19999999999999574,"12250CE":0.3999999999999986,"12300CE":0.1999999999999993,"12350CE":0.20000000000000284,"12400CE":0.15000000000000036,"12450CE":0.15000000000000036,"12500CE":0.049999999999998934,"12550CE":0.15000000000000036,"12600CE":0.10000000000000053,"12650CE":0.09999999999999964,"12700CE":0.10000000000000053,"12800CE":0.10000000000000009}
# import time
# t1 = time.time()
# sample = get_sample_normal_mutation(ltps,2000,bidasks)
# print('Time Taken: ',time.time()-t1)
# pd.Series(sample).hist(bins = 100)

# sell_evs = {"11200PE":5.832154743687838,"11300PE":7.793147475134031,"11400PE":8.330489671001654,"11450PE":8.633991009946456,"11500PE":9.95936973986143,"11550PE":10.65426071155314,"11600PE":9.154548584544287,"11650PE":7.764493114001475,"11700PE":9.163387528692926,"11750PE":9.587741009948202,"11800PE":12.958381790358018,"11850PE":13.306927123180834,"11900PE":16.833649579188855,"11950PE":18.46899387911251,"12000PE":21.685914639933916,"12050PE":16.513961940921902,"12100PE":4.4988098751234595,"12150PE":-13.141122221280778,"12200PE":-18.484095633675178,"12250PE":-29.270940507432467,"12300PE":-34.20591372854996,"12350PE":-47.996518166192885,"12400PE":-38.014679030766935,"12500PE":-42.57447436359284,"12600PE":-38.71517539648948,"12700PE":-40.39994873237582,"12800PE":-36.898201410263155,"12900PE":-65.01528825035528,"13000PE":-25.54928117307903,"13200PE":-34.91612604683547,"13300PE":-29.898892879276197,"13500PE":-12.286383315389326,"13700PE":-33.425390583941805,"13800PE":-8.865602902228423,"10350CE":-47.77013542982788,"10400CE":-43.94513542982929,"10500CE":-38.995135429828096,"10700CE":-48.09513542982824,"10800CE":-47.672465192643934,"10900CE":-48.775000579026276,"11000CE":-32.554589331894114,"11200CE":-34.9129806861408,"11300CE":-33.37698795469487,"11450CE":-47.811144419881586,"11500CE":-25.285765689965775,"11550CE":-34.14087471827447,"11600CE":-27.115586845282973,"11650CE":-28.530642315827084,"11700CE":-25.106747901137492,"11750CE":-29.93239441988217,"11800CE":-18.311753639467728,"11900CE":-13.511485850640733,"11950CE":-12.676141550717249,"12000CE":-7.809220789894509,"12050CE":-12.20617348891087,"12100CE":-24.221325554708752,"12150CE":-34.86125765110922,"12200CE":-44.879231063507895,"12250CE":-52.61607593725923,"12300CE":-58.60104915837566,"12350CE":-62.74165359602139,"12400CE":-63.93481446059805,"12450CE":-63.64289594491116,"12500CE":-62.94460979341986,"12550CE":-61.2703911629686,"12600CE":-59.185310826319316,"12650CE":-56.82115818668749,"12700CE":-54.09508416220362,"12800CE":-50.293336840091705}
# buy_evs = {"11200PE":-6.032154743687837,"11300PE":-8.093147475134032,"11400PE":-8.630489671001655,"11450PE":-9.033991009946462,"11500PE":-10.259369739861434,"11550PE":-11.054260711553146,"11600PE":-9.354548584544283,"11650PE":-8.264493114001475,"11700PE":-9.563387528692932,"11750PE":-10.787741009948205,"11800PE":-13.358381790358024,"11850PE":-14.506927123180851,"11900PE":-17.433649579188877,"11950PE":-19.46899387911251,"12000PE":-22.185914639933916,"12050PE":-17.813961940921914,"12100PE":-4.9988098751234595,"12150PE":2.4411222212807315,"12200PE":16.78409563367519,"12250PE":18.570940507432535,"12300PE":28.905913728550008,"12350PE":19.29651816619284,"12400PE":33.514679030766935,"12500PE":33.07447436359284,"12600PE":26.315175396489614,"12700PE":18.899948732375815,"12800PE":8.598201410263197,"12900PE":-17.28471174964467,"13000PE":14.749281173079076,"13200PE":-2.2838739531643455,"13300PE":-1.201107120723714,"13500PE":-0.013616684610401997,"13700PE":-20.674609416058104,"13800PE":-14.034397097771668,"10350CE":25.970135429828154,"10400CE":27.245135429829475,"10500CE":28.295135429828278,"10700CE":13.395135429827967,"10800CE":11.072465192644025,"10900CE":6.275000579026276,"11000CE":24.85458933189407,"11200CE":20.81298068614089,"11300CE":13.476987954695005,"11450CE":-4.688855580118414,"11500CE":19.685765689965866,"11550CE":0.04087471827444489,"11600CE":18.715586845282996,"11650CE":15.530642315827084,"11700CE":15.306747901137538,"11750CE":12.932394419882169,"11800CE":15.311753639467728,"11900CE":11.411485850640767,"11950CE":6.576141550717283,"12000CE":6.60922078989452,"12050CE":10.70617348891087,"12100CE":23.721325554708752,"12150CE":33.961257651109214,"12200CE":44.4792310635079,"12250CE":51.816075937259235,"12300CE":58.20104915837565,"12350CE":62.34165359602139,"12400CE":63.63481446059805,"12450CE":63.34289594491116,"12500CE":62.84460979341986,"12550CE":60.9703911629686,"12600CE":58.98531082631931,"12650CE":56.62115818668749,"12700CE":53.89508416220362,"12800CE":50.0933368400917}
# ronit_evs = {}
# for item in sell_evs.keys():
#     ronit_evs['-'+item] = sell_evs[item]
# for item in buy_evs.keys():
#     ronit_evs[item] = buy_evs[item]
#
# sample = get_sample(ltps,5000,bidasks)
# evs = get_evs_from_dist(sample,ltps,bidasks)
#
# for item in ronit_evs.keys():
#     if item in evs.keys():
#         print(item, ronit_evs[item], evs[item])