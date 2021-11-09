import os, re, pickle, sys
from datetime import date, datetime


import psycopg2

def connect(host,database,user,password,port):
	conn = psycopg2.connect(
	    host=host,
	    database=database,
	    user=user,
	    password=password,
        port=port)
	return conn


def table_exists(tablename):
    tablename = '_' + str(tablename)
    cur = connect('localhost', 'tickdata', 'postgres', '123','5433').cursor()

    q = "SELECT  * FROM information_schema.tables where table_name = '%s'" % tablename
    cur.execute(q)
    return bool(cur.rowcount)


# In[3]:


def insert_data(tablename, data, timestamp):
    timestamp = datetime.strptime(timestamp, "%m%d%Y-%H%M%S")
    tablename = '_' + str(tablename)
    #     print('Inserting data for :',tablename)
    conn = connect('localhost', 'tickdata', 'postgres', '123','5433')
    cur = conn.cursor()
    myDict = {}
    myDict['tradable'] = data['tradable']
    myDict['mode'] = data['mode']
    myDict['instrument_token'] = data['instrument_token']
    myDict['last_quantity'] = data['last_quantity']
    myDict['average_price'] = data['average_price']
    myDict['volume'] = data['volume']
    myDict['buy_quantity'] = data['buy_quantity']
    myDict['sell_quantity'] = data['sell_quantity']
    myDict['open'] = data['ohlc']['open']
    myDict['high'] = data['ohlc']['high']
    myDict['low'] = data['ohlc']['low']
    myDict['close'] = data['ohlc']['close']
    myDict['change'] = data['change']
    myDict['last_trade_time'] = data['last_trade_time']
    myDict['oi'] = data['oi']
    myDict['oi_day_high'] = data['oi_day_high']
    myDict['oi_day_low'] = data['oi_day_low']
    myDict['timestamp'] = timestamp
    myDict['buy_depth_quantity_0'] = data['depth']['buy'][0]['quantity']
    myDict['buy_depth_quantity_1'] = data['depth']['buy'][1]['quantity']
    myDict['buy_depth_quantity_2'] = data['depth']['buy'][2]['quantity']
    myDict['buy_depth_quantity_3'] = data['depth']['buy'][3]['quantity']
    myDict['buy_depth_quantity_4'] = data['depth']['buy'][4]['quantity']
    myDict['sell_depth_quantity_0'] = data['depth']['sell'][0]['quantity']
    myDict['sell_depth_quantity_1'] = data['depth']['sell'][1]['quantity']
    myDict['sell_depth_quantity_2'] = data['depth']['sell'][2]['quantity']
    myDict['sell_depth_quantity_3'] = data['depth']['sell'][3]['quantity']
    myDict['sell_depth_quantity_4'] = data['depth']['sell'][4]['quantity']

    myDict['buy_depth_price_0'] = data['depth']['buy'][0]['price']
    myDict['buy_depth_price_1'] = data['depth']['buy'][1]['price']
    myDict['buy_depth_price_2'] = data['depth']['buy'][2]['price']
    myDict['buy_depth_price_3'] = data['depth']['buy'][3]['price']
    myDict['buy_depth_price_4'] = data['depth']['buy'][4]['price']
    myDict['sell_depth_price_0'] = data['depth']['sell'][0]['price']
    myDict['sell_depth_price_1'] = data['depth']['sell'][1]['price']
    myDict['sell_depth_price_2'] = data['depth']['sell'][2]['price']
    myDict['sell_depth_price_3'] = data['depth']['sell'][3]['price']
    myDict['sell_depth_price_4'] = data['depth']['sell'][4]['price']

    myDict['buy_depth_orders_0'] = data['depth']['buy'][0]['orders']
    myDict['buy_depth_orders_1'] = data['depth']['buy'][1]['orders']
    myDict['buy_depth_orders_2'] = data['depth']['buy'][2]['orders']
    myDict['buy_depth_orders_3'] = data['depth']['buy'][3]['orders']
    myDict['buy_depth_orders_4'] = data['depth']['buy'][4]['orders']
    myDict['sell_depth_orders_0'] = data['depth']['sell'][0]['orders']
    myDict['sell_depth_orders_1'] = data['depth']['sell'][1]['orders']
    myDict['sell_depth_orders_2'] = data['depth']['sell'][2]['orders']
    myDict['sell_depth_orders_3'] = data['depth']['sell'][3]['orders']
    myDict['sell_depth_orders_4'] = data['depth']['sell'][4]['orders']

    cols = list(myDict.keys())
    vals = [myDict[x] for x in cols]

    vals_str_list = "%s" * len(vals)
    vals_str = ",".join("%s" for x in vals)
    cols_str = ','.join([str(x) for x in cols])

    cur.execute("INSERT INTO {tablename} ({cols}) VALUES ({vals_str})".format(tablename=tablename, cols=cols_str,
                                                                              vals_str=vals_str), vals)
    conn.commit()


# In[4]:


def create_table(tablename):
    tablename = '_' + str(tablename)
    #     print('Creating table for :',tablename)
    conn = connect('localhost', 'tickdata', 'postgres', '123','5433')
    cur = conn.cursor()
    col_names = {'tradable': 'BOOLEAN', 'mode': 'TEXT', 'instrument_token': 'INT', 'last_quantity': 'INT',
                 'average_price': 'REAL', 'volume': 'BIGINT', 'buy_quantity': 'BIGINT', 'sell_quantity': 'BIGINT',
                 'open': 'REAL', 'high': 'REAL', 'low': 'REAL', 'close': 'REAL', 'change': 'REAL',
                 'last_trade_time': 'TIMESTAMP', 'oi': 'BIGINT', 'oi_day_high': 'BIGINT', 'oi_day_low': 'BIGINT',
                 'timestamp': 'TIMESTAMP', 'buy_depth_quantity_0': 'INT', 'buy_depth_quantity_1': 'INT',
                 'buy_depth_quantity_2': 'INT', 'buy_depth_quantity_3': 'INT', 'buy_depth_quantity_4': 'INT',
                 'sell_depth_quantity_0': 'INT', 'sell_depth_quantity_1': 'INT', 'sell_depth_quantity_2': 'INT',
                 'sell_depth_quantity_3': 'INT', 'sell_depth_quantity_4': 'INT', 'buy_depth_price_0': 'INT',
                 'buy_depth_price_1': 'INT', 'buy_depth_price_2': 'INT', 'buy_depth_price_3': 'INT',
                 'buy_depth_price_4': 'INT', 'sell_depth_price_0': 'INT', 'sell_depth_price_1': 'INT',
                 'sell_depth_price_2': 'INT', 'sell_depth_price_3': 'INT', 'sell_depth_price_4': 'INT',
                 'buy_depth_orders_0': 'INT', 'buy_depth_orders_1': 'INT', 'buy_depth_orders_2': 'INT',
                 'buy_depth_orders_3': 'INT', 'buy_depth_orders_4': 'INT', 'sell_depth_orders_0': 'INT',
                 'sell_depth_orders_1': 'INT', 'sell_depth_orders_2': 'INT', 'sell_depth_orders_3': 'INT',
                 'sell_depth_orders_4': 'INT'}
    s = '('
    for ind in col_names:
        s += ind + ' '
        s += col_names[ind]
        if ind != 'sell_depth_orders_4':
            s += ','
    s += ');'
    q = 'CREATE TABLE %s' % tablename
    q += s
    q = q.rstrip(',')
    cur.execute(q)
    constraint = 'timestamp_pk' + tablename
    q = 'ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(timestamp);' % (tablename, constraint)
    cur.execute(q)
    conn.commit()


# In[ ]:


folders = os.listdir()
folders.sort()
for folder in folders:
    if folder.startswith('oldticks'):
        print(folder)
        files = os.listdir(folder)
        files.sort()
        total = len(files)
        c = 1
        for file in files:
            print(c, '/', total)
            c += 1
            # try:
            pklfile = open(folder + '/' + file, 'rb')
            data = pickle.load(pklfile)
            for token in list(data.keys()):
                if table_exists(token):
                    insert_data(token, data[token], file)
                else:
                    create_table(token)
                    insert_data(token, data[token], file)
            # except:
            #     print(sys.exc_info()[0])