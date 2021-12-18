import pickle
import pandas as pd
import numpy as np
import os
import function_store2 as fs
import datetime
import xlwings as xw
from plone.memoize import forever
from functools import lru_cache
book = xw.Book('xlwings.xlsx')
sheet = book.sheets['credit_5%_allvix']
sheet.clear_contents()
pbelow_dir = 'ev_backtest_data'

expiry_files = os.listdir(pbelow_dir)

def get_pbelow(expiry):
    pbelow_file = pbelow_dir + '/' + expiry
    pbelow = pickle.load(open(pbelow_file, 'rb'))
    return pbelow

# Finding out which bear spreads are profitable
def common_non_zero_keys(d1, d2):
    for key in list(d1.keys()):
        if d1[key] == 0:
            d1.pop(key)
    for key in list(d2.keys()):
        if d2[key] == 0:
            d2.pop(key)
    return len(set(d1.keys()) & set(d2.keys()))
result_df = pd.DataFrame()
# sory expiry files by date
expiry_files.sort(key=lambda x: datetime.datetime.strptime(x, 'NIFTY_%d%b%Y_optltps.pickle'))
traded_dates = []
oi_change_wait_minutes = 180
min_common_ois = 20
for file in expiry_files:
    print(file)
    pb = get_pbelow(file)
    expiry = datetime.datetime.strptime(file.split('.')[0].split('_')[1], '%d%b%Y').date()
    print(len(pb))
    past_ois = {}
    timestamplist = [i for i in list(pb.keys()) if i.second == 0 and i.minute == 0 and i.hour>9]
    for timestamp in timestamplist:
        if timestamp.date() in traded_dates:
            continue
        pbnow = pb[timestamp]
        # try:
        # put_spreads, call_spreads = fs.spread_evs_from_pbelow(pbnow['ltps'],pbnow['ois'], pbnow['probability_below_x'],0.03)
        past_ois[timestamp] = pbnow['ois']
        past_oi_tslist = list(past_ois.keys())
        past_oi_tslist.sort(reverse=True)
        # check if timestamp is atleast x minutes after previous timestamp
        for last_timestamp in past_oi_tslist:
            if last_timestamp not in list(past_ois.keys()):
                continue
            if (timestamp - last_timestamp).seconds/60 >= oi_change_wait_minutes:
                old_ois = past_ois[last_timestamp]
                for del_ts in past_oi_tslist:
                    if (timestamp - del_ts).seconds/60 >= oi_change_wait_minutes:
                        past_ois.pop(del_ts)
        pbnow['ltps'],pbnow['ois'] = fs.filter_opt_ltps(pbnow['ltps'], pbnow['ois'],0.05)
        print(len(pbnow['ois']),len(pbnow['ltps']))
        if 'old_ois' not in locals():
            continue
        elif common_non_zero_keys(pbnow['ois'], old_ois) < min_common_ois:
            continue
        distance_from_put, distance_from_call = fs.distance_from_max_oi_change(pbnow['ltps'], pbnow['ois'], old_ois)
        len_opt_ltps = len(pbnow['ltps'])
        if len_opt_ltps < 26 or distance_from_put > 0:
            continue
        dist = fs.nifty_distribution_custom(pbnow['vix']*.95,pbnow['vix']*1.05,fs.find_trading_sessions(timestamp,expiry),
                                                                                         fs.merged_df.copy()[fs.merged_df['nifty_date']<timestamp])
        if len(dist) < 3000:
            continue
        put_spreads, call_spreads = fs.spread_evs_from_dist_montecarlo(pbnow['ltps'],pbnow['ois'],dist,0.05)
        put_spreads,call_spreads = fs.add_debit_spreads(put_spreads,call_spreads)
        if not len(put_spreads) or not len(call_spreads):
            continue
        # except:
        #     continue
        keymax_puts = max(zip(put_spreads.values(), put_spreads.keys()))[1]
        valmax_puts = max(zip(put_spreads.values(), put_spreads.keys()))[0]
        keymax_calls = max(zip(call_spreads.values(), call_spreads.keys()))[1]
        valmax_calls = max(zip(call_spreads.values(), call_spreads.keys()))[0]
        keymax = keymax_puts if valmax_puts > valmax_calls else keymax_calls
        valmax = valmax_puts if valmax_puts > valmax_calls else valmax_calls
        contracts = keymax.split('-')
        if 'PE' in keymax:
            spread = keymax.replace('PE','').split('-')
            if spread[0] > spread[1]:
                spread_type = 'Debit'
                stance = 'Bearish'
            else:
                spread_type = 'Credit'
                stance = 'Bullish'
        elif 'CE' in keymax:
            spread = keymax.replace('CE','').split('-')
            if spread[0] > spread[1]:
                spread_type = 'Credit'
                stance = 'Bearish'
            else:
                spread_type = 'Debit'
                stance = 'Bullish'
        if stance == 'Bearish':
            continue
        bidask_adjust = sum([pbnow['bidasks'][c] for c in contracts])/2
        transaction_cost = 1.15
        result = fs.find_spread_result(keymax,pbnow['index_expiry_price'],pbnow['ltps']) - bidask_adjust - transaction_cost*2
        days_to_expiry = (expiry - timestamp.date()).days
        nr = {'timestamp': timestamp, 'keymax': keymax,'spread_type':spread_type,'stance':stance, 'valmax': valmax, 'result': result, 'call/put': 'put' if valmax_puts > valmax_calls else 'call',
              'days_to_expiry': days_to_expiry, 'expiry': expiry, 'vix': pbnow['vix'], 'distance_from_put': distance_from_put, 'distance_from_call': distance_from_call,'no. of options': len_opt_ltps}
        result_df = result_df.append(nr, ignore_index=True)
        result_df.sort_values(by=['timestamp'], inplace=True)
        result_df['cum_ev'] = result_df['valmax'].cumsum()
        result_df['cum_result'] = result_df['result'].cumsum()
        traded_dates.append(timestamp.date())
        # dist_df,sumdist = fs.pbelow_to_distribution(pbnow['probability_below_x_raw'])
        print(nr)
        result_df.sort_values(by=['timestamp'], inplace=True)
        sheet.range('A1').value = result_df
        # sheet.range('S1:U2000').clear_contents()
        # crange = 'U1:U'+str(len(dist_df)+1)
        # sheet.charts[1].set_source_data(sheet.range(crange))
        # sheet.range('S1').value = dist_df
# plot_series = result_df['result'][result_df['stance'] == 'Bullish'][result_df['days_to_expiry'] < 3]
# plot_series.reset_index(drop=True, inplace=True)
# plot_series.cumsum().plot()



