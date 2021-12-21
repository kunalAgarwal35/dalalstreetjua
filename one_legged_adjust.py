import pickle
import os
import opt_chain_probmodel as ocp
import hist_and_dist as hd
import numpy as np
import pandas as pd
import datetime
import time
import traceback
import cProfile

def get_expiry_files(pbelow_dir):
    expiry_files = os.listdir(pbelow_dir)
    if pbelow_dir == 'mongo_cache':
        expiry_files = [i.strftime('NIFTY_%d%b%Y.pkl') for i in
                        sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pkl') for x in expiry_files if 'NIFTY' in x])]
        expiry_files = dict(zip(expiry_files, [datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pkl') for x in expiry_files]))
    elif pbelow_dir == 'ev_backtest_data':
        expiry_files = [i.strftime('NIFTY_%d%b%Y_optltps.pickle') for i in
                        sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y_optltps.pickle') for x in expiry_files])]
        expiry_files = dict(zip(expiry_files, [datetime.datetime.strptime(x, 'NIFTY_%d%b%Y_optltps.pickle') for x in expiry_files if 'NIFTY' in x]))
    elif pbelow_dir == 'mongo_cache_reborn':
        expiry_files = [i.strftime('NIFTY_%d%b%Y.pkl') for i in
                        sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pkl') for x in expiry_files if 'NIFTY' in x])]
        expiry_files = dict(zip(expiry_files, [datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pkl') for x in expiry_files]))
    elif pbelow_dir == 'mongodb_cached_files_anim':
        expiry_files = [i.strftime('NIFTY_%d%b%Y.pickle') for i in
                        sorted([datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pickle') for x in expiry_files if 'NIFTY' in x])]
        expiry_files = dict(zip(expiry_files, [datetime.datetime.strptime(x, 'NIFTY_%d%b%Y.pickle') for x in expiry_files]))
    else:
        print('Invalid pbelow_dir')
        expiry_files = {}
    return expiry_files


def get_expiry_data(pbelow_dir, fname):
    expiry_data = pickle.load(open(os.path.join(pbelow_dir, fname), 'rb'))
    return expiry_data


def transaction_cost_options(buy_price,sell_price, qty):
    qty = abs(qty)
    # Zerodha Transaction Cost Options
    brokerage = 20
    # stt is on sell side (on premium)
    stt = 0.0005
    transaction_charges = 0.00053
    # gst is on brokerage + transaction charges
    gst = 0.18
    sebi_charges_per_10mil = 10
    stamp_charges_per_10mil = 300
    turnover = qty * (buy_price + sell_price)
    brokerage = brokerage * 2
    stt_total = sell_price * qty * stt
    exchange_txn_charge = turnover * transaction_charges
    tot_gst = (brokerage + exchange_txn_charge) * gst
    sebi_charges = turnover/10000000 * sebi_charges_per_10mil
    stamp_duty = buy_price*qty/10000000 * stamp_charges_per_10mil
    total_cost = brokerage + stt_total + exchange_txn_charge + tot_gst + sebi_charges + stamp_duty
    return total_cost


def find_trades(open_positions,contract_evs,spot,trade_range, percentile,min_contracts):
    call_sell_dict, call_buy_dict, put_sell_dict, put_buy_dict = dict(), dict(), dict(), dict()
    # shrinking_contract_ev by 5 times:
    for contract in contract_evs.keys():
        contract_evs[contract] = contract_evs[contract] * 0.2
    for contract, ev in contract_evs.items():
        if '-' in contract:
            if 'CE' in contract:
                strike = float(contract.split('-')[1].split('CE')[0])
                if abs(np.log(strike/spot)) < trade_range:
                    call_sell_dict[contract.replace('-','')] = ev
                if len(open_positions) > 0 and contract.replace('-','') in open_positions.keys():
                    if open_positions[contract.replace('-','')] < 0:
                        current_call_sell_ev = ev
                        current_call_sell_contract = contract.replace('-','')
            elif 'PE' in contract:
                strike = float(contract.split('-')[1].split('PE')[0])
                if abs(np.log(strike/spot)) < trade_range:
                    put_sell_dict[contract.replace('-','')] = ev
                if len(open_positions) > 0 and contract.replace('-','') in open_positions.keys():
                    if open_positions[contract.replace('-','')] < 0:
                        current_put_sell_ev = ev
                        current_put_sell_contract = contract.replace('-','')
        else:
            if 'CE' in contract:
                strike = float(contract.split('CE')[0])
                if abs(np.log(strike/spot)) < trade_range:
                    call_buy_dict[contract] = ev
                if len(open_positions) > 0 and contract in open_positions.keys():
                    if open_positions[contract] > 0:
                        current_call_buy_ev = ev
                        current_call_buy_contract = contract
            elif 'PE' in contract:
                strike = float(contract.split('PE')[0])
                if abs(np.log(strike/spot)) < trade_range:
                    put_buy_dict[contract] = ev
                if len(open_positions) > 0 and contract in open_positions.keys():
                    if open_positions[contract] > 0:
                        current_put_buy_ev = ev
                        current_put_buy_contract = contract

    call_sell_dict = dict(sorted(call_sell_dict.items(), key=lambda x: x[1]))
    call_buy_dict = dict(sorted(call_buy_dict.items(), key=lambda x: x[1]))
    put_sell_dict = dict(sorted(put_sell_dict.items(), key=lambda x: x[1]))
    put_buy_dict = dict(sorted(put_buy_dict.items(), key=lambda x: x[1]))
    if len(call_sell_dict) > min_contracts:
        call_sell_threshold = np.percentile(np.array([x[1] for x in call_sell_dict.items()]), percentile) - 2
        highest_ev_call_sell = max(call_sell_dict, key=call_sell_dict.get)
    else:
        call_sell_threshold = -99999999
    if len(call_buy_dict) > min_contracts:
        call_buy_threshold = np.percentile(np.array([x[1] for x in call_buy_dict.items()]), percentile) - 2
        highest_ev_call_buy = max(call_buy_dict, key=call_buy_dict.get)
    else:
        call_buy_threshold = -99999999
    if len(put_sell_dict) > min_contracts:
        put_sell_threshold = np.percentile(np.array([x[1] for x in put_sell_dict.items()]), percentile) - 2
        highest_ev_put_sell = max(put_sell_dict, key=put_sell_dict.get)
    else:
        put_sell_threshold = -99999999
    if len(put_buy_dict) > min_contracts:
        put_buy_threshold = np.percentile(np.array([x[1] for x in put_buy_dict.items()]), percentile) - 2
        highest_ev_put_buy = max(put_buy_dict, key=put_buy_dict.get)
    else:
        put_buy_threshold = -99999999
    tradelist = {}

    if len(open_positions) > 0:
        if 'current_call_sell_ev' in locals() and 'highest_ev_call_sell' in locals():
            if current_call_sell_ev < call_sell_threshold:
                if current_call_sell_contract not in tradelist.keys():
                    tradelist[current_call_sell_contract] = 1
                else:
                    tradelist[current_call_sell_contract] += 1
                if highest_ev_call_sell not in tradelist.keys():
                    tradelist[highest_ev_call_sell] = -1
                else:
                    tradelist[highest_ev_call_sell] += -1

        if 'current_call_buy_ev' in locals() and 'highest_ev_call_buy' in locals():
            if current_call_buy_ev < call_buy_threshold:
                if current_call_buy_contract not in tradelist.keys():
                    tradelist[current_call_buy_contract] = -1
                else:
                    tradelist[current_call_buy_contract] += -1
                if highest_ev_call_buy not in tradelist.keys():
                    tradelist[highest_ev_call_buy] = 1
                else:
                    tradelist[highest_ev_call_buy] += 1

        if 'current_put_sell_ev' in locals() and 'highest_ev_put_sell' in locals():
            if current_put_sell_ev < put_sell_threshold:
                if current_put_sell_contract not in tradelist.keys():
                    tradelist[current_put_sell_contract] = 1
                else:
                    tradelist[current_put_sell_contract] += 1
                if highest_ev_put_sell not in tradelist.keys():
                    tradelist[highest_ev_put_sell] = -1
                else:
                    tradelist[highest_ev_put_sell] += -1

        if 'current_put_buy_ev' in locals() and 'highest_ev_put_buy' in locals():
            if current_put_buy_ev < put_buy_threshold:
                if current_put_buy_contract not in tradelist.keys():
                    tradelist[current_put_buy_contract] = -1
                else:
                    tradelist[current_put_buy_contract] += -1
                if highest_ev_put_buy not in tradelist.keys():
                    tradelist[highest_ev_put_buy] = 1
                else:
                    tradelist[highest_ev_put_buy] += 1
        if len(open_positions) == 2:
            ops = str(open_positions)
            if 'CE' not in ops:
                if 'highest_ev_call_sell' in locals() and 'highest_ev_call_buy' in locals():
                    if highest_ev_call_sell not in tradelist.keys():
                        tradelist[highest_ev_call_sell] = -1
                    else:
                        tradelist[highest_ev_call_sell] -= 1
                    if highest_ev_call_buy not in tradelist.keys():
                        tradelist[highest_ev_call_buy] = 1
                    else:
                        tradelist[highest_ev_call_buy] += 1
            if 'PE' not in ops:
                if 'highest_ev_put_sell' in locals() and 'highest_ev_put_buy' in locals():
                    if highest_ev_put_sell not in tradelist.keys():
                        tradelist[highest_ev_put_sell] = -1
                    else:
                        tradelist[highest_ev_put_sell] -= 1
                    if highest_ev_put_buy not in tradelist.keys():
                        tradelist[highest_ev_put_buy] = 1
                    else:
                        tradelist[highest_ev_put_buy] += 1

    else:
        if len(call_sell_dict) > min_contracts and len(call_buy_dict) > min_contracts and len(put_sell_dict) > min_contracts and len(put_buy_dict) > min_contracts:
            tradelist[highest_ev_call_sell] = -1
            tradelist[highest_ev_call_buy] = 1
            tradelist[highest_ev_put_sell] = -1
            tradelist[highest_ev_put_buy] = 1

    return tradelist


def find_expiry_values(index_expiry_price,open_positions):
    expiry_values = dict()
    for contract in open_positions.keys():
        if 'CE' in contract:
            strike = float(contract.split('CE')[0])
            if strike > index_expiry_price:
                expiry_values[contract] = 0
            else:
                expiry_values[contract] = index_expiry_price - strike
        elif 'PE' in contract:
            strike = float(contract.split('PE')[0])
            if strike < index_expiry_price:
                expiry_values[contract] = 0
            else:
                expiry_values[contract] = strike - index_expiry_price
    return expiry_values




def backtest_adjust(expiry_data,expiry, trade_range,percentile, nlots,lot_size,last_trade_time,min_contracts):
    # expiry data is the expiry dictionary saved by mongo.py
    # expiry is the expiry date
    # trade_range is the max % difference of strike from spot price to be traded
    # percentile is the percentile threshold for adjusting each leg of the trade
    # nlots is the number of lots to be traded
    # lot_size is the size of each lot
    # last_trade_time is the cuttoff time to adjust trades on expiry day
    # min_contracts is the minimum number of puts and calls in a tick to consider trading
    open_positions = dict()
    ledger = pd.DataFrame(columns=['Timestamp','Instrument','Price','Qty','Brokerage'])
    timestamps = list(expiry_data.keys())
    timestamps.sort()
    if type(expiry) == type(datetime.datetime.now()):
        expiry_date = expiry.date()
    else:
        expiry_date = expiry
    for timestamp in timestamps:
        if timestamp.minute % 5 != 0 or timestamp.second != 0:
            continue
        try:
            timedata = expiry_data[timestamp]
            timedata['ltps'],timedata['bidasks'] = hd.fs.augment_optiondata(timedata['ltps'],timedata['bidasks'])
            if len(timedata['ltps']) < min_contracts*2:
                continue
            print(timestamp)
            # print(open_positions)
            if len(open_positions) == 0 or len(open_positions) == 2 or len(open_positions) == 4:
                ops = str(open_positions)
                if len(open_positions) == 2:
                    if 'CE' not in ops or 'PE' not in ops:
                        pass
                    else:
                        breakpoint()
            else:
                breakpoint()
            if timestamp.time() < last_trade_time or timestamp.date() < expiry_date:
                dist = hd.get_combo_dist(timedata['ltps'],timestamp,expiry_date,vix_range=0.1,chain_weight=0.75, bidasks = timedata['bidasks'])
                if not len(dist):
                    continue
                contract_evs = hd.get_evs_from_dist(dist,timedata['ltps'],timedata['bidasks'])
                # contract_evs = ocp.get_contract_evs(timedata['ltps'], timedata['bidasks'], 1, 1)
                if not len(contract_evs):
                    continue
                spot = ocp.get_syn_spot(timedata['ltps'])
                tradelist = find_trades(open_positions,contract_evs,spot,trade_range, percentile,min_contracts)
                if len(tradelist) > 0:
                    print('Tradelist:', tradelist)
                    if len(open_positions) == 0:
                        print('Initiating Open Positions')
                        for contract in tradelist.keys():
                            open_positions[contract] = tradelist[contract]*nlots*lot_size
                            if tradelist[contract] > 0:
                                price = timedata['ltps'][contract] + timedata['bidasks'][contract]
                                qty = tradelist[contract]*nlots*lot_size
                            else:
                                price = timedata['ltps'][contract] - timedata['bidasks'][contract]
                                qty = tradelist[contract]*nlots*lot_size
                            new_row = {'Timestamp':timestamp,'Instrument':contract+str(expiry_date),'Price':price,'Qty':qty,'Brokerage':0}
                            ledger = ledger.append(new_row,ignore_index=True)
                        print(open_positions)
                    else:
                        print('Adjusting Open Positions')
                        for contract in tradelist.keys():
                            if contract in list(open_positions.keys()):
                                open_positions[contract] += tradelist[contract]*nlots*lot_size
                                if open_positions[contract] == 0:
                                    del open_positions[contract]
                                last_trade = ledger.loc[ledger['Instrument'] == contract + str(expiry_date)]
                                if tradelist[contract] > 0:
                                    price = timedata['ltps'][contract] + timedata['bidasks'][contract]
                                    qty = tradelist[contract]*nlots*lot_size
                                    buy_price = price
                                    sell_price = float(last_trade['Price'].iloc[-1])
                                else:
                                    price = timedata['ltps'][contract] - timedata['bidasks'][contract]
                                    qty = tradelist[contract]*nlots*lot_size
                                    buy_price = float(last_trade['Price'].iloc[-1])
                                    sell_price = price
                                # Calculating brokerage
                                brokerage = transaction_cost_options(buy_price,sell_price,qty)
                                new_row = {'Timestamp':timestamp,'Instrument':contract+str(expiry_date),'Price':price,'Qty':qty,'Brokerage':brokerage}
                                ledger = ledger.append(new_row,ignore_index=True)
                            else:
                                open_positions[contract] = tradelist[contract]*nlots*lot_size
                                if tradelist[contract] > 0:
                                    price = timedata['ltps'][contract] + timedata['bidasks'][contract]
                                    qty = tradelist[contract]*nlots*lot_size
                                else:
                                    price = timedata['ltps'][contract] - timedata['bidasks'][contract]
                                    qty = tradelist[contract]*nlots*lot_size
                                new_row = {'Timestamp':timestamp,'Instrument':contract+str(expiry_date),'Price':price,'Qty':qty,'Brokerage':0}
                                ledger = ledger.append(new_row,ignore_index=True)
                        print(open_positions)
            # else of expiry date after last_trade_time
            else:
                pass
        except Exception as e:
            # print traceback
            print('Error message: ' + str(e))
            print(traceback.format_exc())
            breakpoint()
    #Squaring off final positions
    expiry_price = hd.fs.get_nifty_close(expiry_date)
    expiry_values = find_expiry_values(expiry_price,open_positions)
    tradelist = dict()
    for contract in open_positions.keys():
        tradelist[contract] = -open_positions[contract]/(lot_size*nlots)
    if len(tradelist) > 0:
        for contract in tradelist.keys():
            last_trade = ledger.loc[ledger['Instrument'] == contract + str(expiry_date)]
            if tradelist[contract] > 0:
                price = expiry_values[contract]
                qty = tradelist[contract]*nlots*lot_size
                buy_price = price
                sell_price = float(last_trade['Price'].iloc[-1])
            else:
                price = expiry_values[contract]
                qty = tradelist[contract]*nlots*lot_size
                buy_price = float(last_trade['Price'].iloc[-1])
                sell_price = price
            # Calculating brokerage
            brokerage = transaction_cost_options(buy_price,sell_price,qty)
            new_row = {'Timestamp':timestamp,'Instrument':contract+str(expiry_date),'Price':price,'Qty':qty,'Brokerage':brokerage}
            ledger = ledger.append(new_row,ignore_index=True)
    return ledger



pbelow_dir = 'mongodb_cached_files_anim'
ledger_dir = 'one_legged_ledgers'
expiry_files = get_expiry_files(pbelow_dir)
for i in range(len(expiry_files)):
    if 'ledger'+str(list(expiry_files.keys())[i]) in os.listdir(ledger_dir):
        print('Skipping ' + str(list(expiry_files.keys())[i]))
        continue
    ledger = backtest_adjust(expiry_data = get_expiry_data(pbelow_dir,list(expiry_files.keys())[i]),trade_range=0.05,expiry=list(expiry_files.values())[i],
                             lot_size=50,nlots=1,last_trade_time=datetime.time(hour=13,minute=30),percentile=80,min_contracts=15)
    pickle.dump(ledger,open(ledger_dir+'/'+'ledger'+str(list(expiry_files.keys())[i]),'wb'))














