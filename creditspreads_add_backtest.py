import pickle
import pandas as pd
import numpy as np
import os
import function_store2 as fs
import datetime
import xlwings as xw

book = xw.Book('xlwings.xlsx')
sheet = book.sheets['credit_spreads_add']
pbelow_dir = 'ev_backtest_pbelow'

expiry_files = os.listdir(pbelow_dir)

def get_pbelow(expiry):
    pbelow_file = pbelow_dir + '/' + expiry
    pbelow = pickle.load(open(pbelow_file, 'rb'))
    return pbelow

# pb = get_pbelow(expiry_files[1])
result_df = pd.DataFrame()
# sory expiry files by date
expiry_files.sort(key=lambda x: datetime.datetime.strptime(x, 'NIFTY_%d%b%Y_optltps.pickle'))
for file in expiry_files:
    print(file)
    pb = get_pbelow(file)
    expiry = datetime.datetime.strptime(file.split('.')[0].split('_')[1], '%d%b%Y').date()
    print(len(pb))
    timestamplist = [ i for i in list(pb.keys()) if i.hour == 9 and i.minute == 30 and i.second == 0]
    for timestamp in timestamplist:
        pbnow = pb[timestamp]
        try:
            # put_spreads, call_spreads = fs.spread_evs_from_pbelow(pbnow['ltps'],pbnow['ois'], pbnow['probability_below_x'],0.03)
            dist = fs.nifty_distribution_custom(pbnow['vix']*.99,pbnow['vix']*1.05,fs.find_trading_sessions(timestamp,expiry),
                                                                                             fs.merged_df.copy()[fs.merged_df['nifty_date']<timestamp])
            put_spreads, call_spreads = fs.spread_evs_from_dist_montecarlo(pbnow['ltps'],pbnow['ois'],dist,0.05)
            # put_spreads,call_spreads = fs.add_debit_spreads(put_spreads,call_spreads)
            if not len(put_spreads) or not len(call_spreads):
                continue
        except:
            continue
        keymax_puts = max(zip(put_spreads.values(), put_spreads.keys()))[1]
        valmax_puts = max(zip(put_spreads.values(), put_spreads.keys()))[0]
        keymax_calls = max(zip(call_spreads.values(), call_spreads.keys()))[1]
        valmax_calls = max(zip(call_spreads.values(), call_spreads.keys()))[0]
        keymax = keymax_puts + '&' + keymax_calls
        valmax = valmax_puts + valmax_calls
        contracts = keymax.split('-')
        bidask_adjust = sum([pbnow['bidasks'][c] for c in keymax_puts.split('-')]) + sum([pbnow['bidasks'][c] for c in keymax_calls.split('-')])
        transaction_cost = 1.15
        trade_put = keymax_puts.split('-')[0] + '-' + keymax_puts.split('-')[1]
        trade_call = keymax_calls.split('-')[0] + '-' + keymax_calls.split('-')[1]
        distance_from_put,distance_from_call = fs.distance_from_max_ois(pbnow['ltps'],pbnow['ois'])
        result = fs.find_spread_result(trade_put,pbnow['index_expiry_price'],pbnow['ltps']) + fs.find_spread_result(trade_put,pbnow['index_expiry_price'],pbnow['ltps'])
        result -= (4*transaction_cost)+bidask_adjust
        days_to_expiry = (expiry - timestamp.date()).days
        # if 'PE' in keymax:
        #     spread = keymax.replace('PE','').split('-')
        #     if spread[0] > spread[1]:
        #         spread_type = 'Debit'
        #         stance = 'Bearish'
        #     else:
        #         spread_type = 'Credit'
        #         stance = 'Bullish'
        # elif 'CE' in keymax:
        #     spread = keymax.replace('CE','').split('-')
        #     if spread[0] > spread[1]:
        #         spread_type = 'Credit'
        #         stance = 'Bearish'
        #     else:
        #         spread_type = 'Debit'
        #         stance = 'Bullish'
        spot = fs.get_syn_spot(pbnow['ltps'])
        change_till_expiry = 100*np.log(pbnow['index_expiry_price']/spot)
        nr = {'timestamp': timestamp, 'keymax': keymax, 'valmax': valmax, 'result': result, 'days_to_expiry': days_to_expiry,
              'expiry': expiry,'vix':pbnow['vix'],'spot':spot,'change_till_expiry':change_till_expiry,
              'slippage':bidask_adjust,'transaction_cost':transaction_cost*4,'len_dist':len(dist),
              'call_distance':distance_from_call,'put_distance':distance_from_put}
        result_df = result_df.append(nr, ignore_index=True)
        result_df.sort_values(by=['timestamp'], inplace=True)
        result_df['cum_ev'] = result_df['valmax'].cumsum()
        result_df['cum_result'] = result_df['result'].cumsum()
        result_df['cum_slip_cost'] = (result_df['transaction_cost'] + result_df['slippage']).cumsum()
        dist_df,sumdist = fs.pbelow_to_distribution(pbnow['probability_below_x_raw'])
        print(nr)
        sheet.range('A1').value = result_df
        # sheet.range('S1:U2000').clear_contents()
        # crange = 'U1:U'+str(len(dist_df)+1)
        # sheet.charts[1].set_source_data(sheet.range(crange))
        # sheet.range('S1').value = dist_df




