import pickle
import traceback

import pandas as pd
import numpy as np
import os
import function_store2 as fs
import datetime
import xlwings as xw
from plone.memoize import forever
from functools import lru_cache
import traceback
import opt_chain_probmodel as opc
book = xw.Book('xlwings.xlsx')
sheet = book.sheets['opchmodel']
sheet.clear_contents()
pbelow_dir = 'mongo_cache'



def get_stance(keymax):
    if 'PE' in keymax:
        spread = keymax.replace('PE', '').split('-')
        if spread[0] > spread[1]:
            spread_type = 'Debit'
            stance = 'Bearish'
        else:
            spread_type = 'Credit'
            stance = 'Bullish'
    elif 'CE' in keymax:
        spread = keymax.replace('CE', '').split('-')
        if spread[0] > spread[1]:
            spread_type = 'Credit'
            stance = 'Bearish'
        else:
            spread_type = 'Debit'
            stance = 'Bullish'
    return stance, spread_type

def get_best_spread(stance,put_spreads,call_spreads):
    # Sort spreads dicts by values
    put_spreads = dict(sorted(put_spreads.items(), key=lambda x: x[1], reverse=True))
    call_spreads = dict(sorted(call_spreads.items(), key=lambda x: x[1], reverse=True))
    # Get best spread
    for spread in put_spreads.keys():
        put_stance = get_stance(spread)
        if put_stance[0] == stance:
            best_put_spread = spread
            break
    for spread in call_spreads.keys():
        call_stance = get_stance(spread)
        if call_stance[0] == stance:
            best_call_spread = spread
            break
    if put_spreads[best_put_spread] > call_spreads[best_call_spread]:
        best_spread = best_put_spread
        spread_type = put_stance[1]
        valmax = put_spreads[best_put_spread]
    else:
        best_spread = best_call_spread
        spread_type = call_stance[1]
        valmax = call_spreads[best_call_spread]
    return best_spread, spread_type, valmax

def update_old_ois(timestamp,pb):
    timestamplist = list(pb.keys())
    old_ois = {}
    # filter for last 10 minutes
    timestamplist = [x for x in timestamplist if timestamp - datetime.timedelta(minutes=10) < x < timestamp]
    for timestamp in timestamplist:
        pbnow = pb[timestamp]['ois']
        for key in pbnow.keys():
            old_ois[key] = pbnow[key]
    return old_ois



def get_pbelow(expiry):
    pbelow_file = pbelow_dir + '/' + expiry
    pbelow = pickle.load(open(pbelow_file, 'rb'))
    return pbelow
#Input is both credit and debit spreads


# pb = get_pbelow(expiry_files[1])
result_df = pd.DataFrame()
# sory expiry files by date
expiry_files = os.listdir(pbelow_dir)
# expiry_files = [i.strftime('NIFTY_%d%b%Y.pkl') for i in sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pkl') for x in expiry_files])]
if pbelow_dir == 'mongo_cache':
    expiry_files = [i.strftime('NIFTY_%d%b%Y.pkl') for i in sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pkl') for x in expiry_files])]
else:
    expiry_files = [i.strftime('NIFTY_%d%b%Y_optltps.pickle') for i in sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y_optltps.pickle') for x in expiry_files])]
traded_dates = dict()

model_range = 1
trade_range = 0.05
for file in os.listdir(pbelow_dir):
    print(file)
    if pbelow_dir == 'mongo_cache':
        pb_str = file.split('_')[1].replace('.pkl', '')
    else:
        pb_str = file.split('_')[1].replace('.pickle', '')
    expiry = datetime.datetime.strptime(pb_str, '%d%b%Y').date()
    pb = get_pbelow(file)
    print(len(pb))
    timestamplist = [ i for i in list(pb.keys()) if i.hour > 9 and i.minute % 20 == 0 and i.second == 0]
    for timestamp in timestamplist:
        if timestamp.date() in traded_dates.keys():
            if traded_dates[timestamp.date()] > 2:
                continue
        pbnow = pb[timestamp]
        # if not len(pbnow['ltps'])>0 or not len(pbnow['ois'])>0:
        if not len(pbnow['ltps'])>0 or not len(pbnow['ois'])>0:
            continue

        try:
            old_ois = update_old_ois(timestamp,pb)
            if not len(pbnow['ltps'])>0 or not len(pbnow['ois'])>0:
                continue
            try:
                put_spreads, call_spreads = opc.get_evs(pbnow['ltps'],pbnow['bidasks'],model_range,trade_range)
            except:
                print('Error in EV calculation for ' + str(timestamp))
                continue

            put_spreads,call_spreads = fs.add_debit_spreads(put_spreads,call_spreads)

            if not len(put_spreads) or not len(call_spreads):
                continue
        except Exception:
            print(traceback.format_exc())
            print(timestamp)
            breakpoint()
            continue
        days_to_expiry = (expiry - timestamp.date()).days
        distance_from_put, distance_from_call = fs.distance_from_max_ois(pbnow['ltps'], old_ois)
        # Trade Exclusions
        if days_to_expiry > 20 or days_to_expiry < 1 or len(pbnow['ltps']) < 25:
            continue
        # if distance_from_put < 0 or distance_from_call < 0:
        #     continue

        stance = 'Bullish'
        trade_spread,spread_type, valmax = get_best_spread(stance,put_spreads,call_spreads)
        stance = 'Bearish'
        trade_spread_2, spread_type_2, valmax_2 = get_best_spread(stance, put_spreads, call_spreads)
        if valmax_2 > valmax:
            trade_spread = trade_spread_2
            spread_type = spread_type_2
            valmax = valmax_2
        else:
            stance = 'Bullish'

        contracts = trade_spread.split('-')
        bidask_adjust = sum([pbnow['bidasks'][c] for c in contracts])
        transaction_cost = 1.15
        result = fs.find_spread_result(trade_spread,pbnow['index_expiry_price'],pbnow['ltps']) - bidask_adjust - transaction_cost*2
        len_opt_ltps = len(pbnow['ltps'])
        nr = {'timestamp': timestamp, 'trade': trade_spread,'spread_type':spread_type,'stance':stance, 'valmax': valmax, 'result': result,
              'days_to_expiry': days_to_expiry, 'expiry': expiry, 'vix':0, 'distance_from_put': distance_from_put,
              'distance_from_call': distance_from_call,'no. of options': len_opt_ltps,'spot':pbnow['spot']}
        result_df = result_df.append(nr, ignore_index=True)
        result_df.sort_values(by=['timestamp'], inplace=True)
        result_df['cum_ev'] = result_df['valmax'].cumsum()
        result_df['cum_result'] = result_df['result'].cumsum()
        result_df.sort_values(by=['timestamp'], inplace=True)
        if timestamp.date() not in traded_dates.keys():
            traded_dates[timestamp.date()] = 1
        else:
            traded_dates[timestamp.date()] += 1
        old_ois = {}
        # dist_df,sumdist = fs.pbelow_to_distribution(pbnow['probability_below_x_raw'])
        print(nr)
        sheet.range('A1').value = result_df
        # sheet.range('S1:U2000').clear_contents()
        # crange = 'U1:U'+str(len(dist_df)+1)
        # sheet.charts[1].set_source_data(sheet.range(crange))
        # sheet.range('S1').value = dist_df


    # result_df[result_df['stance'] == 'Bullish'].plot(x='distance_from_put', y='result', kind='scatter')

    ''' 
    Findings: 
    1. If distance_from_put is negative, then highest EV bullish spread is printing money.
    2. If distance_from_put is positive, then highest EV bullish spread is random and net losing money. 
    3. Everything is normally distributed, there is no edge, fuck off. 
    
    '''
plot_series = result_df['result'][result_df['stance'] == 'Bullish'][result_df['distance_from_put'] < 0]
plot_series = result_df['result'][result_df['distance_from_put']>0][result_df['distance_from_call'] > 0]
plot_series = result_df['result'][((result_df['stance'] == 'Bullish') & (result_df['distance_from_put']<0)) | ((result_df['stance'] == 'Bearish') & (result_df['distance_from_call']>2*result_df['distance_from_put']))]
plot_df = result_df[((result_df['stance'] == 'Bullish') & (result_df['distance_from_put']<0)) | ((result_df['stance'] == 'Bearish') & (result_df['distance_from_call']>2*result_df['distance_from_put']))]
plot_series.reset_index(drop=True, inplace=True)
plot_series.cumsum().plot(grid=True, title='Cumulative Result')

# for exp in plot_df['expiry'].unique():
#     rdf = result_df['result'][(result_df['vix'] < 18) & (result_df['stance']=='Bearish')& (result_df['expiry']==exp)]
#     print(exp,len(rdf), rdf.sum())
# nifty_end = []
#
#
# df_nif = pd.DataFrame()
# df_nif['end'] = np.arange(15500,19000,1)
#
# df_nif['above'] = [len(nifty_end[nifty_end > i])/len(nifty_end) for i in df_nif['end']]
# df_nif['below'] = [len(nifty_end[nifty_end < i])/len(nifty_end) for i in df_nif['end']]
# pairwise = pd.DataFrame()
# pairwise['above'] = df_nif['end']
# for i in df_nif['end']:
#     pairwise[i] = df_nif['above'][df_nif['end'] == i] - df_nif['below'][df_nif['end'] == i]
#
# # change all negative values to zero
# pairwise[pairwise < 0] = 0
# # change nan to zero
# pairwise[pairwise.isnull()] = 0



