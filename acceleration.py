import pickle, os
import datetime
import pandas as pd
import psycopg2
import numpy as np
import math
import time
import mpl_charts as mc

ticklist = os.listdir('ticks')
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in ticklist]


def transaction_cost(entry_price, exit_price, lot_size):
    total_entry = entry_price * lot_size
    total_exit = exit_price * lot_size
    turnover = total_entry + total_exit

    # Charges
    brokerage = min(0.0003 * total_entry, 20) + min(0.0003 * total_exit, 20)
    stt = 0.0001 * total_exit
    exchange_tax = 0.00002 * turnover
    gst = 0.18 * (brokerage + exchange_tax)
    sebi_charges = turnover / 1000000
    stamp_charges = min(0.00002 * total_entry, 2 * total_entry / 100000)
    return brokerage + stt + exchange_tax + gst + sebi_charges + stamp_charges


def updateticklist():
    ticklist = os.listdir('ticks')
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in ticklist]


def connect(host, database, user, password, port):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port)
    return conn
def get_lot_size_dict(filename):
    instruments = pickle.load(open(filename,'rb'))
    d = {}
    for instrument in instruments:
        d[instrument['instrument_token']] = instrument['lot_size']
        ts[instrument['instrument_token']] = instrument['tradingsymbol']
    return d

def load_data(instrument_token, oi_thresh):
    if type(instrument_token) == 'int':
        instrument_token = str(instrument_token)
    conn = connect('localhost', 'tickdata', 'postgres', '123', '5433')
    cur = conn.cursor()
    cols = ['timestamp', 'buy_depth_price_0', 'buy_depth_price_1', 'buy_depth_price_2', 'buy_depth_price_3',
            'buy_depth_price_4', 'sell_depth_price_0', 'sell_depth_price_1', 'sell_depth_price_2', 'sell_depth_price_3',
            'sell_depth_price_4', 'last_price', 'oi']
    colnames = ','.join([x for x in cols])
    q = 'SELECT {colnames} from {tablename}'.format(colnames=colnames, tablename='_' + str(instrument_token))
    cur.execute(q)
    data = cur.fetchall()
    df = pd.DataFrame(data=data, columns=cols)

    drop_rows = []
    p = df.copy()
    p['OIC'] = 0.0
    p['PC'] = 0.0
    p['PC/OIC'] = 0.0
    p['timestamp'] = pd.to_datetime(p['timestamp'], format="%d/%m/%Y %H:%M:%S")
    p = p.sort_values(by=['timestamp'])
    p = p.reset_index(drop=True)
    day_start = datetime.time(9, 15, 0)
    day_end = datetime.time(15, 30, 0)

    for i in range(len(p)):
        t = p.iloc[i]['timestamp']
        if i < len(p) and (t.time() < day_start or t.time() > day_end):
            drop_rows.append(i)

    p = p.drop(drop_rows)
    p = p.reset_index(drop=True)

    convert_dict = {'buy_depth_price_0': float, 'buy_depth_price_1': float, 'buy_depth_price_2': float,
                    'buy_depth_price_3': float, 'buy_depth_price_4': float, 'sell_depth_price_0': float,
                    'sell_depth_price_1': float, 'sell_depth_price_2': float, 'sell_depth_price_3': float,
                    'sell_depth_price_4': float, 'last_price': float, 'oi': float}
    p = p.astype(convert_dict)
    base = 0
    i = 0
    drop_rows = []
    while base < len(p) - 1:
        i = base + 1
        new_value = p.iloc[i]['oi']
        old_value = p.iloc[base]['oi']
        while i < len(p) and abs(100 * np.log(new_value / old_value)) < oi_thresh:
            drop_rows.append(i)
            new_value = p.iloc[i]['oi']
            old_value = p.iloc[base]['oi']
            i += 1

        base = i

    p = p.drop(drop_rows)
    p = p.reset_index(drop=True)
    drop_rows = []

    for i in range(1, len(p)):
        p.iat[i, 13] = p.iloc[i]['oi'] - p.iloc[i - 1]['oi']
        p.iat[i, 14] = p.iloc[i]['last_price'] - p.iloc[i - 1]['last_price']
        if p.iloc[i]['OIC'] != 0:
            p.iat[i, 15] = p.iloc[i]['PC'] / p.iloc[i]['OIC']
        if p.iloc[i]['OIC'] == 0 or math.isnan(p.iloc[i]['OIC']):
            drop_rows.append(i)
        t = p.iloc[i]['timestamp']
        if t.time() < day_start or t.time() > day_end:
            drop_rows.append(i)

    p = p.drop(drop_rows)
    p = p.reset_index(drop=True)
    return p
ts={}
lot_size_dict = get_lot_size_dict('April_kite.instruments')
may_lot_size_dict = get_lot_size_dict('May.instruments')
for token in may_lot_size_dict.keys():
    lot_size_dict[token]=may_lot_size_dict[token]

def backtest(tradingsymbol,oi_thres):
    for key in ts.keys():
        if tradingsymbol in ts[key]:
            break
    df = load_data(key, oi_thres)
    df['ratio'] = df['PC/OIC'] / df['PC/OIC'].shift(1)
    df = df[2:]
    df = df.reset_index(drop=True)
    df=df.sort_values(by='timestamp')
    intrade = 0
    n=0
    entry=0
    tradesheet=pd.DataFrame(columns=['type','entry','exit','gross','tc','net'])
    for row in range(len(df)):
        if df.iloc[row]['ratio'] > 1 and df.iloc[row]['OIC'] > 0 and not intrade:
            intrade = 1
            start_time = df.iloc[row]['timestamp']
            if df.iloc[row]['PC'] > 0:
                entry=df.iloc[row]['sell_depth_price_0']
                name = 'Long at ' + str(df.iloc[row]['sell_depth_price_0'])
                print('Entering Long at:', start_time)
            else:
                entry = df.iloc[row]['buy_depth_price_0']
                name = 'Short at ' + str(df.iloc[row]['buy_depth_price_0'])
                print('Entering Short at:', start_time)
        if intrade and df.iloc[row]['ratio'] < 1:
            end_time = df.iloc[row]['timestamp']
            print('Exiting Trade at:', end_time)
            intrade = 0
            if 'Long' in name:
                exit=df.iloc[row]['buy_depth_price_0']
                name = name + ', Exit: ' + str(df.iloc[row]['buy_depth_price_0'])
                tsrow={'type':'Long','entry':entry,'exit':exit,'gross':(exit-entry)*lot_size_dict[key],'tc':transaction_cost(entry,exit,lot_size_dict[key])}
                tsrow['net']=tsrow['gross']-tsrow['tc']
                tradesheet=tradesheet.append(tsrow,ignore_index=True)
            else:
                exit = df.iloc[row]['sell_depth_price_0']
                name = name + 'Exit: ' + str(df.iloc[row]['sell_depth_price_0'])
                tsrow = {'type': 'Long', 'entry': entry, 'exit': exit, 'gross': (entry - exit) * lot_size_dict[key],
                         'tc': transaction_cost(entry, exit, lot_size_dict[key])}
                tsrow['net'] = tsrow['gross'] - tsrow['tc']
                tradesheet=tradesheet.append(tsrow, ignore_index=True)
            st=datetime.datetime(start_time.year,start_time.month,start_time.day,start_time.hour,5*(start_time.minute//5))
            et=datetime.datetime(end_time.year,end_time.month,end_time.day,end_time.hour,5*((end_time.minute//5)+1))
            fdate=st.strftime('%Y-%m-%d %H:%M:%S')
            tdate=et.strftime('%Y-%m-%d %H:%M:%S')
            mc.savechart(name,'pcoic/'+ts[key]+str(n),key,fdate,tdate,'5minute',0)
            n=n+1
            tradesheet.to_csv('pcoic/tradesheet.csv',index=False)
