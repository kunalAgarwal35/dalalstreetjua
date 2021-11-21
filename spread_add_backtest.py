import pickle
import pandas as pd
import numpy as np
import os
import function_store2 as fs
import datetime
import xlwings as xw

book = xw.Book('xlwings.xlsx')
sheet = book.sheets['spread_add_bt_hourly_raw']
pbelow_dir = 'ev_backtest_pbelow'

expiry_files = os.listdir(pbelow_dir)

def get_pbelow(expiry):
    pbelow_file = pbelow_dir + '/' + expiry
    pbelow = pickle.load(open(pbelow_file, 'rb'))
    return pbelow

# pb = get_pbelow(expiry_files[1])
result_df = pd.DataFrame()
for file in expiry_files:
    print(file)
    pb = get_pbelow(file)
    expiry = datetime.datetime.strptime(file.split('.')[0].split('_')[1], '%d%b%Y').date()
    print(len(pb))
    timestamplist = [ i for i in list(pb.keys()) if i.hour == 11 and i.minute == 30 and i.second == 0]
    for timestamp in timestamplist:
        pbnow = pb[timestamp]
        try:
            # put_spreads, call_spreads = fs.spread_evs_from_pbelow(pbnow['ltps'],pbnow['ois'], pbnow['probability_below_x'],0.03)
            put_spreads, call_spreads = fs.spread_evs_from_dist(pbnow['ltps'],pbnow['ois'],
                                                                fs.nifty_distribution_custom(pbnow['vix']*0.8,pbnow['vix']*1.2,fs.find_trading_sessions(timestamp,expiry),
                                                                                             fs.merged_df.copy()[fs.merged_df['nifty_date']<timestamp]),0.03)
            put_spreads,call_spreads = fs.add_debit_spreads(put_spreads,call_spreads)
            if not len(put_spreads) or not len(call_spreads):
                continue
        except:
            continue
        keymax_puts = max(zip(put_spreads.values(), put_spreads.keys()))[1]
        valmax_puts = max(zip(put_spreads.values(), put_spreads.keys()))[0]
        keymax_calls = max(zip(call_spreads.values(), call_spreads.keys()))[1]
        valmax_calls = max(zip(call_spreads.values(), call_spreads.keys()))[0]
        keymax = keymax_puts if valmax_puts > valmax_calls else keymax_calls
        valmax = valmax_puts if valmax_puts > valmax_calls else valmax_calls
        contracts = keymax.split('-')
        bidask_adjust = sum([pbnow['bidasks'][c] for c in contracts])
        transaction_cost = 2
        result = fs.find_spread_result(keymax,pbnow['index_expiry_price'],pbnow['ltps']) - bidask_adjust - transaction_cost
        days_to_expiry = (expiry - timestamp.date()).days
        nr = {'timestamp': timestamp, 'keymax': keymax, 'valmax': valmax, 'result': result, 'call/put': 'put' if valmax_puts > valmax_calls else 'call',
              'days_to_expiry': days_to_expiry, 'expiry': expiry}
        result_df = result_df.append(nr, ignore_index=True)
        result_df.sort_values(by=['timestamp'], inplace=True)
        result_df['cum_ev'] = result_df['valmax'].cumsum()
        result_df['cum_result'] = result_df['result'].cumsum()
        dist_df,sumdist = fs.pbelow_to_distribution(pbnow['probability_below_x_raw'])
        print(nr)
        sheet.range('A1').value = result_df
        sheet.range('S1:U2000').clear_contents()
        crange = 'U1:U'+str(len(dist_df)+1)
        sheet.charts[1].set_source_data(sheet.range(crange))
        sheet.range('S1').value = dist_df




