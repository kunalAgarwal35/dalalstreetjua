import pandas as pd
import pickle
import numpy as np
import time
import datetime
import zd
import os

aux_files = 'optionadjust/'
allins = zd.kite.instruments()
def find_expiry():
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100
    for ins in allins:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days
                exp_opt_date = ins['expiry']
    return exp_opt_date
def initiate_variables():
    if 'ledger' in os.listdir(aux_files):
        ledger = pickle.load(open(aux_files+'ledger','rb'))
    else:
        ledger = pd.DataFrame()

    if 'open_positions' in os.listdir(aux_files):
        open_positions = pickle.load(open(aux_files+'open_positions','rb'))
    else:
        open_positions = {}
    return ledger,open_positions
ledger,open_positions = initiate_variables()
def english_to_tradingsymbol():
    engts = {}
    expiry = find_expiry()
    for key in curr_ins.keys():
        if curr_ins[key]['instrument_type'] in ['CE','PE'] and curr_ins[key]['name'] == 'NIFTY' and curr_ins[key]['expiry'] == expiry:
            engts[str(int(curr_ins[key]['strike']))+curr_ins[key]['instrument_type']] = curr_ins[key]['tradingsymbol']

    return curr_ins,engts
curr_ins,engts = english_to_tradingsymbol()

def send_option_market_order(type, token, qty):
    if type == 'buy':
        tt = zd.kite.TRANSACTION_TYPE_BUY
    else:
        tt = zd.kite.TRANSACTION_TYPE_SELL
    try:
        order_id = zd.kite.place_order(tradingsymbol=token,
                                       exchange=zd.kite.EXCHANGE_NFO,
                                       transaction_type=tt,
                                       quantity=qty,
                                       order_type=zd.kite.ORDER_TYPE_MARKET,
                                       product=zd.kite.PRODUCT_NRML,
                                       variety='regular')

        print("Order placed. ID is: {}".format(order_id))
        return order_id
    except Exception as e:
        print("Order placement failed: {}".format(e))
        return order_id
def show_tradelist(tradelist,option_prices):
    global ledger, open_positions
    tradelist = dict(sorted(tradelist.items(), key=lambda item: item[1],reverse=True))
    calls,puts = {},{}
    for key in tradelist.keys():
        if 'CE' in key:
            calls[key] = tradelist[key]
        else:
            puts[key] = tradelist[key]
    for leg in calls.keys():




    if len(tradelist)==0:
        return
    tradelist = trade_tradelist(tradelist,scalp,wait_time)
    if len(tradelist)==0:
        return

    plist = []
    for key in tradelist.keys():
        if key in open_positions.keys():
            open_positions[key] += tradelist[key]
            if open_positions[key] == 0:
                plist.append(key)
        else:
            open_positions[key] = tradelist[key]
    for key in plist:
        open_positions.pop(key)
    for key in tradelist.keys():
        nr = {'timestamp': datetime.datetime.now(), 'expiry': find_expiry(), 'instrument': key,
              'qty': tradelist[key] * 75, 'price': option_ltps[key]}
        ledger = ledger.append(nr, ignore_index=True)
    print(tradelist)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    sqpnl = 0
    for key in open_positions.keys():
        if open_positions[key] == 1:
            sqpnl += option_ltps[key]*75
        else:
            sqpnl -= option_ltps[key]*75
    booked_pnl = {}
    poplist = []
    for contract in print_ledger['instrument']:
        if contract not in booked_pnl.keys():
            if print_ledger[print_ledger['instrument']==contract]['qty'].sum() == 0:
                booked_pnl[contract] = print_ledger[print_ledger['instrument']==contract]['value'].sum()
        else:
            if print_ledger[print_ledger['instrument'] == contract]['qty'].sum() != 0:
               poplist.append(contract)
    for key in poplist:
        booked_pnl.pop(key)
    # for key in tradelist.keys():
    #     if tradelist[key]>0:
    #         otype = 'buy'
    #     else:
    #         otype = 'sell'
    #     token = curr_ins[label_to_token[key]]['tradingsymbol']
    #     send_option_order(otype, token, abs(tradelist[key]*75))
    openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
    s.range('F10').value = booked_pnl
    s.range('H9').value = sum(booked_pnl.values())
    s.range('H2').value = sqpnl
    s.range('H5').value = openpnl
    s.range('Z1').value = print_ledger['value'].sum() + sqpnl
    pickle.dump(open_positions, open('open_option_positions', 'wb'))
    pickle.dump(ledger, open('current_ledger', 'wb'))
