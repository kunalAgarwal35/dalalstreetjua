import pickle
import os
import pandas as pd
import numpy as np
import datetime
import time
import xlwings as xw
import nsepython as nsep

file_path='xlwings.xlsx'
s=xw.Book(file_path).sheets['Sheet4']

market_open = datetime.time(9,15,00)
market_close = datetime.time(15,30,00)


nifty50list = pd.read_csv('ind_nifty50list.csv')['Symbol'].to_list()
nifty50list.pop(nifty50list.index('NIFTY'))

aux_files = 'ledgers/'
def initiate_variables():
    if 'ledger' in os.listdir(aux_files):
        ledger = pickle.load(open(aux_files + 'ledger', 'rb'))
    else:
        ledger = pd.DataFrame()

    if 'intrade_max_list' in os.listdir(aux_files):
        intrade_max_list = pickle.load(open(aux_files + 'intrade_max_list', 'rb'))
    else:
        intrade = {}
        max_b = {}
        intrade_symb = {}
        for symbol in nifty50list:
            intrade[symbol] = 0
            max_b[symbol] = 0
            intrade_symb[symbol] = ''
        intrade_max_list = [intrade,max_b,intrade_symb]
    return ledger, intrade_max_list

def most_liquid_call_option(symbol):
    optch = nsep.option_chain(symbol)['filtered']['data']
    vol = 0
    fce = {}
    for item in optch:
        if 'CE' in item.keys():
            ce = item['CE']
            if ce['openInterest'] > vol:
                vol = ce['totalTradedVolume']
                fce = ce
    return fce
def particular_call_option(symbol):
    global intrade_symb
    optch = nsep.option_chain(symbol)['filtered']['data']
    for item in optch:
        if 'CE' in item.keys():
            ce = item['CE']
            if ce['identifier'] == intrade_symb[symbol]:
                return ce
def ledger_entry(typ,symb):
    global ledger,intrade_symb,intrade,lot_sizes
    if typ == 'buy':
        mlc = most_liquid_call_option(symb)
        price = mlc['askPrice']
        identifier = mlc['identifier']
        strike = int(mlc['strikePrice'])
        under = mlc['underlying']
        qty = lot_sizes[symb]
        expiry = mlc['expiryDate']
        timestamp = datetime.datetime.now()
        intrade_symb[symb] = identifier
    else:
        mlc = particular_call_option(symb)
        price = mlc['bidprice']
        identifier = mlc['identifier']
        qty = -lot_sizes[symb]
        strike = int(mlc['strikePrice'])
        under = mlc['underlying']
        expiry = mlc['expiryDate']
        timestamp = datetime.datetime.now()
    nr = {'timestamp':timestamp,'underlying':under,'strike':strike,'price':price,'qty':qty,'identifier':identifier,'expiry':expiry,'P/L':0}
    if len(ledger)>=2:
        sqnum = (-ledger['qty']*ledger['price']).sum()
        for item in intrade.keys():
            if intrade[item]:
                sqnum += particular_call_option(item)['bidprice']*lot_sizes[item]
        nr['P/L'] = sqnum
    ledger = ledger.append(nr, ignore_index=True)
    s.range('X1').value = ledger
    pickle.dump(ledger,open(aux_files+'ledger','wb'))







lot_sizes = nsep.nse_get_fno_lot_sizes(symbol = 'all',mode = 'list')
ledger, intrade_max_list = initiate_variables()
[intrade,max_b,intrade_symb] = intrade_max_list

out_df = {}
for symbol in nifty50list:
    out_df[symbol] = 0

while market_open<datetime.datetime.now().time()<market_close:
    print('initializing...')
    for symbol in nifty50list:
        try:
            optch = nsep.option_chain(symbol)['filtered']['data']
            spot = float(nsep.nsetools_get_quote(symbol)['lastPrice'])
            net_oi = 0
            product_oi_price = 0
            for item in optch:
                strike = item['strikePrice']
                if 'CE' in item.keys():
                    ce = item['CE']
                    oi = ce['openInterest']
                    price = (ce['bidprice'] + ce['askPrice']) / 2
                    net_oi += oi
                    product_oi_price += oi * (strike + price)
                if 'PE' in item.keys():
                    pe = item['PE']
                    oi = pe['openInterest']
                    price = (pe['bidprice'] + pe['askPrice']) / 2
                    net_oi += oi
                    product_oi_price += oi * (strike - price)
            projected_price = product_oi_price/net_oi
            out = 100*np.log(projected_price/spot)
            out_df[symbol] = out
            s.range('A1').value = out_df
            # print(out)
            if out>4.3:
                if not intrade[symbol]:
                    entry = spot
                    entry_time = datetime.datetime.now()
                    print(symbol,'---BUY--->',str(entry), '(',entry_time,')')
                    time.sleep(1)
                    ledger_entry(typ = 'buy',symb = symbol)
                    intrade[symbol] = 1
                else:
                    if out>max_b[symbol]:
                        max_b[symbol] = out
            elif out<3.9:
                if intrade[symbol]:
                    exit = spot
                    exit_time = datetime.datetime.now()
                    print(symbol, '---SELL--->', str(exit), '(', exit_time, ')')
                    time.sleep(0.5)
                    ledger_entry(typ = 'sell',symb = symbol)
                    intrade[symbol] = 0
                    max_b[symbol] = 0
            pickle.dump([intrade,max_b,intrade_symb],open(aux_files+'intrade_max_list','wb'))

        except Exception as e:
            print(e)



