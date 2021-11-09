import collections
import psycopg2
import os, pickle
from psycopg2 import sql
from collections import OrderedDict
from collections import defaultdict
import sys
import re

def connect(host,database,user,password,port):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port)
    return conn

def get_security_name(path):
    instrument_data = list(pickle.load(open(path, "rb")))
    instrmnt_name = OrderedDict()
    num_tokens = len(instrument_data)
    for i in range(num_tokens):
        index = instrument_data[i-1]['instrument_token']
        security_name = "".join(re.split("[^a-zA-Z]*", instrument_data[i-1]['name']))
        instrmnt_name[index] = security_name
    return instrmnt_name

def table_exists(tablename, connection):
    cursor = connection.cursor()
    exists = False
    table = tablename.lower()
    try:
        q = "SELECT exists(select * from information_schema.tables where table_name ='" + table + "')"
        cursor.execute(q)
        exists = cursor.fetchone()[0]
        #print(exists)
        cursor.close()
    except psycopg2.Error as e:
        print(e)
    return exists

def create_table(tablename, connection):
    cursor = connection.cursor()
    q1_create_table = """CREATE TABLE IF NOT EXISTS %s (
                                           tradable varchar(255) NULL,
                                           mode varchar(255) NULL, 
                                           timestamp TIMESTAMP NOT NULL,
                                           instrument_token INTEGER NULL, 
                                           last_price DOUBLE PRECISION NULL, 
                                           last_quantity DOUBLE PRECISION NULL, 
                                           average_price DOUBLE PRECISION NULL, 
                                           volume DOUBLE PRECISION NULL, 
                                           buy_quantity DOUBLE PRECISION NULL, 
                                           sell_quantity DOUBLE PRECISION NULL, 
                                           open DOUBLE PRECISION NULL,
                                           high DOUBLE PRECISION NULL, 
                                           low DOUBLE PRECISION NULL, 
                                           close DOUBLE PRECISION NULL, 
                                           change DOUBLE PRECISION NULL, 
                                           last_trade_time TIMESTAMP NULL, 
                                           oi DOUBLE PRECISION NULL, 
                                           oi_day_high DOUBLE PRECISION NULL,
                                           oi_day_low DOUBLE PRECISION NULL,
                                           depth_buy_quantity_1 DOUBLE PRECISION NULL,
                                           depth_buy_price_1 DOUBLE PRECISION NULL,
                                           depth_buy_orders_1 DOUBLE PRECISION NULL,
                                           depth_buy_quantity_2 DOUBLE PRECISION NULL,
                                           depth_buy_price_2 DOUBLE PRECISION NULL,
                                           depth_buy_orders_2 DOUBLE PRECISION NULL,
                                           depth_buy_quantity_3 DOUBLE PRECISION NULL,
                                           depth_buy_price_3 DOUBLE PRECISION NULL,
                                           depth_buy_orders_3 DOUBLE PRECISION NULL,
                                           depth_buy_quantity_4 DOUBLE PRECISION NULL,
                                           depth_buy_price_4 DOUBLE PRECISION NULL,
                                           depth_buy_orders_4 DOUBLE PRECISION NULL,
                                           depth_buy_quantity_5 DOUBLE PRECISION NULL,
                                           depth_buy_price_5 DOUBLE PRECISION NULL,
                                           depth_buy_orders_5 DOUBLE PRECISION NULL,
                                           depth_sell_quantity_1 DOUBLE PRECISION NULL,
                                           depth_sell_price_1 DOUBLE PRECISION NULL,
                                           depth_sell_orders_1 DOUBLE PRECISION NULL,
                                           depth_sell_quantity_2 DOUBLE PRECISION NULL,
                                           depth_sell_price_2 DOUBLE PRECISION NULL,
                                           depth_sell_orders_2 DOUBLE PRECISION NULL,
                                           depth_sell_quantity_3 DOUBLE PRECISION NULL,
                                           depth_sell_price_3 DOUBLE PRECISION NULL,
                                           depth_sell_orders_3 DOUBLE PRECISION NULL,
                                           depth_sell_quantity_4 DOUBLE PRECISION NULL,
                                           depth_sell_price_4 DOUBLE PRECISION NULL,
                                           depth_sell_orders_4 DOUBLE PRECISION NULL,
                                           depth_sell_quantity_5 DOUBLE PRECISION NULL,
                                           depth_sell_price_5 DOUBLE PRECISION NULL,
                                           depth_sell_orders_5 DOUBLE PRECISION NULL,
                                           PRIMARY KEY (timestamp)
                                           );"""%tablename
    q1_create_hypertable = "SELECT create_hypertable('%s', 'timestamp')"%tablename
    cursor.execute(q1_create_table)
    cursor.execute(q1_create_hypertable)
    connection.commit()
    cursor.close()

def unroll(token_data, tablename):

    cols = list(token_data.keys())
    vals = [token_data[x] for x in cols]
    ohlc = token_data['ohlc']
    depth = token_data['depth']
    open = str(ohlc['open'])
    high = str(ohlc['high'])
    low = str(ohlc['low'])
    close = str(ohlc['close'])
    depth_buy_1 = depth['buy'][0]
    depth_buy_2 = depth['buy'][1]
    depth_buy_3 = depth['buy'][2]
    depth_buy_4 = depth['buy'][3]
    depth_buy_5 = depth['buy'][4]
    depth_sell_1 = depth['sell'][0]
    depth_sell_2 = depth['sell'][1]
    depth_sell_3 = depth['sell'][2]
    depth_sell_4 = depth['sell'][3]
    depth_sell_5 = depth['sell'][4]
    depth_buy_quantity_1 = depth_buy_1['quantity']
    depth_buy_quantity_2 = depth_buy_2['quantity']
    depth_buy_quantity_3 = depth_buy_3['quantity']
    depth_buy_quantity_4 = depth_buy_4['quantity']
    depth_buy_quantity_5 = depth_buy_5['quantity']
    depth_buy_price_1 = depth_buy_1['price']
    depth_buy_price_2 = depth_buy_2['price']
    depth_buy_price_3 = depth_buy_3['price']
    depth_buy_price_4 = depth_buy_4['price']
    depth_buy_price_5 = depth_buy_5['price']
    depth_buy_orders_1 = depth_buy_1['orders']
    depth_buy_orders_2 = depth_buy_2['orders']
    depth_buy_orders_3 = depth_buy_3['orders']
    depth_buy_orders_4 = depth_buy_4['orders']
    depth_buy_orders_5 = depth_buy_5['orders']
    depth_sell_quantity_1 = depth_sell_1['quantity']
    depth_sell_quantity_2 = depth_sell_2['quantity']
    depth_sell_quantity_3 = depth_sell_3['quantity']
    depth_sell_quantity_4 = depth_sell_4['quantity']
    depth_sell_quantity_5 = depth_sell_5['quantity']
    depth_sell_price_1 = depth_sell_1['price']
    depth_sell_price_2 = depth_sell_2['price']
    depth_sell_price_3 = depth_sell_3['price']
    depth_sell_price_4 = depth_sell_4['price']
    depth_sell_price_5 = depth_sell_5['price']
    depth_sell_orders_1 = depth_sell_1['orders']
    depth_sell_orders_2 = depth_sell_2['price']
    depth_sell_orders_3 = depth_sell_3['price']
    depth_sell_orders_4 = depth_sell_4['price']
    depth_sell_orders_5 = depth_sell_5['price']
    tup = {}
    tup = [token_data['tradable'],token_data['mode'], token_data['timestamp'],
                                 token_data['instrument_token'], token_data['last_price'],
                                 token_data['last_quantity'], token_data['average_price'],
                                 token_data['volume'], token_data['buy_quantity'],
                                 token_data['sell_quantity'],token_data['ohlc']['open'], token_data['ohlc']['high'], token_data['ohlc']['low'], token_data['ohlc']['close'],
                                 token_data['change'],
                                 token_data['last_trade_time'], token_data['oi'],
                                 token_data['oi_day_high'], token_data['oi_day_low'],
                                 depth['buy'][0]['quantity'], depth_buy_price_1, depth_buy_orders_1,
                                 depth['buy'][1]['quantity'], depth_buy_price_2, depth_buy_orders_2,
                                 depth['buy'][2]['quantity'], depth_buy_price_3, depth_buy_orders_3,
                                 depth_buy_quantity_4, depth_buy_price_4, depth_buy_orders_4,
                                 depth_buy_quantity_5, depth_buy_price_5, depth_buy_orders_5,
                                 depth_sell_quantity_1, depth_sell_price_1, depth_sell_orders_1,
                                 depth_sell_quantity_2, depth_sell_price_2, depth_sell_orders_2,
                                 depth_sell_quantity_3, depth_sell_price_3, depth_sell_orders_3,
                                 depth_sell_quantity_4, depth_sell_price_4, depth_sell_orders_4,
                                 depth_sell_quantity_5, depth_sell_price_5, depth_sell_orders_5,
                                 ]
    #print(tup)
    return tup

def insert_data(data, connection):
    cursor = connection.cursor()
    for table in data.keys():

        args_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data[table][x]).decode("utf-8") for x in data[table].keys())
        #print(args_str)
        #query = "INSERT INTO %s values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            cursor.execute("INSERT INTO " + table.lower() + " values " + args_str)
            connection.commit()
        except psycopg2.errors.UniqueViolation as e:
            print(e)
            connection.rollback()
            pass
        #break
    cursor.close()





connection = connect("localhost", "futures_tick_data", "postgres", "postpass@123", "5433")
#connection = connect("localhost", "test", "postgres", "welcome123", "5433")
month = 'april_2021'
instrmnt_path = "C:/Kiteconnect/instruments/" +  month + ".instruments"
#instrmnt_path = "C:/Users/ravi/Downloads" + "/" +  month + ".instruments"

instrmnt_token_dctnry = get_security_name(instrmnt_path)
#print(instrmnt_token_dctnry)

tick_file_path = "E:/Kiteconnect/mayticks"
#tick_file_path = "C:/Users/ravi\Downloads/tick_files_test"
tick_files_names = os.listdir(tick_file_path)
total_files = len(tick_files_names)
file_num = 0
token_num = 0
tup = collections.defaultdict(dict)
batch_size = 1000

for tick_file_name in tick_files_names:
    tick_file_object = pickle.load(open(tick_file_path + '/' + tick_file_name, 'rb'))
    instrument_tokens = tick_file_object.keys()
    num_tokens = len(instrument_tokens)
    for token in instrument_tokens:
        tablename = instrmnt_token_dctnry[token]
        #print(tablename)
        if table_exists(tablename, connection):
            tup[tablename][token] = unroll(tick_file_object[token], connection)
        else:
            create_table(tablename, connection)
            tup[tablename][token] = unroll(tick_file_object[token], connection)
        token_num += 1
        #print("tokens done in %s %s / %s " %(tick_file_name,token_num, num_tokens))
    file_num += 1
    if not file_num % batch_size or file_num == len(tick_files_names):
        insert_data(tup, connection)

    #print(tup.keys())
    print("Files done %s / %s" %(file_num, total_files))
#    break