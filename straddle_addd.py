import pickle
import pandas as pd
import numpy as np
import os
import function_store2 as fs
import datetime
import xlwings as xw

book = xw.Book('xlwings.xlsx')
sheet = book.sheets['straddle_add (2)']
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
            put_evs,call_evs = fs.straddle_from_dist_montecarlo(pbnow['ltps'],pbnow['ois'],
                                                                fs.nifty_distribution_custom(pbnow['vix']*.99,pbnow['vix']*1.05,fs.find_trading_sessions(timestamp,expiry),
                                                                                             fs.merged_df.copy()[fs.merged_df['nifty_date']<timestamp]),0.05)
            # put_spreads,call_spreads = fs.add_debit_spreads(put_spreads,call_spreads)
            if not len(put_evs) or not len(call_evs):
                continue
        except:
            continue
        keymin_puts = max(zip(put_evs.values(), put_evs.keys()))[1]
        keymin_calls = max(zip(call_evs.values(), call_evs.keys()))[1]
        valmin_puts = max(zip(put_evs.values(), put_evs.keys()))[0]
        valmin_calls = max(zip(call_evs.values(), call_evs.keys()))[0]

        contracts = [keymin_puts,keymin_calls]
        bidask_adjust = sum([pbnow['bidasks'][c] for c in contracts])
        transaction_cost = 2
        result = fs.find_existing_positions_result({keymin_puts:1},pbnow['ltps'][keymin_puts],pbnow['index_expiry_price']) + \
                 fs.find_existing_positions_result({keymin_calls:1},pbnow['ltps'][keymin_calls],pbnow['index_expiry_price'])
        result -= (2*transaction_cost)+bidask_adjust
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
        nr = {'timestamp': timestamp, 'keymax': '-'+keymin_puts + ' & -' +keymin_calls, 'valmax': -1*(valmin_calls+valmin_puts),
              'result': result, 'days_to_expiry': days_to_expiry,'expiry': expiry,'vix':pbnow['vix'],
              'spot':spot,'change_till_expiry':change_till_expiry,'slippage':bidask_adjust,'transaction_cost':transaction_cost*4}
        result_df = result_df.append(nr, ignore_index=True)
        result_df.sort_values(by=['timestamp'], inplace=True)
        result_df['cum_ev'] = result_df['valmax'].cumsum()
        result_df['cum_result'] = result_df['result'].cumsum()
        result_df['cum_slip_cost'] = (result_df['transaction_cost'] + result_df['slippage']).cumsum()
        dist_df,sumdist = fs.pbelow_to_distribution(pbnow['probability_below_x_raw'])
        print(nr)
        sheet.range('A1').value = result_df
        sheet.range('S1:U2000').clear_contents()
        crange = 'U1:U'+str(len(dist_df)+1)
        # sheet.charts[1].set_source_data(sheet.range(crange))
        sheet.range('S1').value = dist_df




