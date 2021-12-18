import threading
import traceback
import function_store2 as fs
import time
import datetime
import pymongo
import os
import pandas as pd
import requests
import pickle
from multiprocessing import Pool
from itertools import repeat


client = pymongo.MongoClient("mongodb://localhost:27017/")
# Database Name
db = client["tick_data_csv"]
cache = 'mongo_cache_reborn'


def get_previous_month(date):
    return date - datetime.timedelta(days=date.day)


def get_syn_spot(opt_ltps):
    strikes = [int(opt.replace('CE', '').replace('PE', ''))
               for opt in opt_ltps.keys()]
    strikes = list(set(strikes))
    strikes.sort()
    # remove top and bottom 3
    if len(strikes) > 5:
        strikes = strikes[3:-3]
    spot = pd.Series([strike - opt_ltps[str(strike)+'PE'] + opt_ltps[str(strike)+'CE']
                     for strike in strikes if str(strike)+'CE' in opt_ltps.keys() and str(strike)+'PE' in opt_ltps.keys()]).mean()
    return spot


def date_to_connectionname(date, deritvative_type):
    if deritvative_type == "FUT":
        con_name = date.strftime("%b_%Y").upper() + '_futures'
    elif deritvative_type == "OPT":
        con_name = date.strftime("%b_%Y").upper() + '_options'
    else:
        print("Error: Derivative type not recognized")
        return None
    return con_name

def add_to_dictionary(dictionary, contract, value):
    dictionary[contract] = value

def add_data_to_dictionary(dictionary, instrument, expiry, start_date, col, i):
    print("Storing data for {} in cache".format(start_date))
    x = col.find({'sym': instrument.upper(), 'exp_d': expiry,
              "ts": {"$lt": start_date + datetime.timedelta(days=1), "$gt": start_date}},
             {'sym': 1, 'bp': 1, 'sp': 1, 'oi': 1, 'str': 1, 'op_typ': 1, 'ts': 1, '_id': 0})
    dictionary[i] = pd.DataFrame(list(x))
    # print(dictionary.keys())

def get_options_dictionary(instrument, expiry):
    # expiry is a datetime object with expiry date and 0 hours 0 minutes 0 seconds
    acceptable_delay = 5
    # con_name = date_to_connectionname(expiry, "OPT")
    # Next line to be commneted out
    # excluded_months = ['FEB_2019_options', 'NOV_2021_options', 'DEC_2021_options']
    # Bug in the above line. FEB_2019 is the folder name, the connection name should be FEB_2019_options
    # if con_name in excluded_months:
    #     return {}
    fname = instrument+'_'+expiry.strftime("%d%b%Y")+'.pkl'
    if fname in os.listdir(cache):
        with open(cache+'/'+fname, 'rb') as f:
            return pickle.load(f)
    expiry_price = fs.get_nifty_close(expiry.date())
    # Number of days you want the data for
    start_date = expiry - datetime.timedelta(days=61)
    data_dict = dict()
    i = 0
    next_days_data = dict()
    empty_dates = list()
    while True:
        start_date = start_date + datetime.timedelta(days=1)
        col = db[date_to_connectionname(start_date, "OPT")]
        if start_date > expiry:
            break
        t1 = time.time()
        if start_date in next_days_data.keys():
            df = next_days_data[start_date]
        else:
            x = col.find({'sym': instrument.upper(), 'exp_d': expiry,
                          "ts": {"$lt": start_date + datetime.timedelta(days=1), "$gt": start_date}},
                         {'sym': 1, 'bp': 1, 'sp': 1, 'oi': 1, 'str': 1, 'op_typ': 1, 'ts': 1, '_id': 0})
            df = pd.DataFrame(list(x))

        i += 1
        thread = threading.Thread(target=add_data_to_dictionary, args=(next_days_data, instrument, expiry, start_date + datetime.timedelta(days=1), col, start_date + datetime.timedelta(days=1)))
        thread.start()
        try:
            df['bp'] = df['bp'].astype(float)
            df['sp'] = df['sp'].astype(float)
            df['oi'] = df['oi'].astype(float)
        except:

            if df.shape[0] != 0:
                print("Error in converting to float")
                df.to_csv('errors/error_{}.csv'.format(i))
            else:
                empty_dates.append(start_date)
                with open("empty_dates.txt", "a+") as f:
                    f.write(start_date.strftime("%d%b%Y")+'\n')
            continue


        for key in list(next_days_data.keys()):
            if key < start_date:
                del next_days_data[key]

        print(df.shape)
        if df.shape[0] == 0:
            print(start_date, "is not a trading date")
            continue
        print('Processing: ', start_date, 'Time Taken: ', time.time()-t1)
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.sort_values(by=['ts'])
        unique_timestamps = pd.to_datetime(pd.Series([i for i in pd.to_datetime(pd.Series(df['ts'].unique())) if i.second % acceptable_delay == 0]))
        df.reset_index(drop=True, inplace=True)
        df['acceptable_ts'] = [i.replace(second=(i.second//acceptable_delay)*acceptable_delay) for i in df['ts']]
        df['bp'].astype(float)
        df['sp'].astype(float)
        df['oi'].astype(int)
        df = df[(df['bp'] > 0) & (df['sp'] > 0) & (df['oi'] > 0)]
        df['sym_ltp'] = (df['bp'] + df['sp'])/2
        df['contract'] = df['str'].astype(int).apply(str)+df['op_typ']
        df['bidasks'] = df['sp'] - df['bp']
        for timestamp in unique_timestamps:
            collective_dict = {}
            sub_df = df[df['acceptable_ts'] == timestamp]
            if not len(sub_df):
                continue
            ltps = {}
            ois = {}
            bidasks = {}
            sub_df.apply(lambda row: add_to_dictionary(ltps, row['contract'], row['sym_ltp']), axis=1)
            sub_df.apply(lambda row: add_to_dictionary(ois, row['contract'], row['oi']), axis=1)
            sub_df.apply(lambda row: add_to_dictionary(bidasks, row['contract'], row['bidasks']), axis=1)
            if len(ltps) > 10 and len(ois) > 0 and len(bidasks) > 0:
                try:
                    collective_dict['ltps'] = ltps
                    collective_dict['ois'] = ois
                    collective_dict['bidasks'] = bidasks
                    collective_dict['spot'] = get_syn_spot(ltps)
                    collective_dict['index_expiry_price'] = expiry_price
                    # collective_dict['vix'] = fs.get_vix_close(timestamp)
                    data_dict[timestamp] = collective_dict
                except Exception:
                    print(traceback.format_exc())


    print("Time taken to get options dictionary: ", time.time() - t1)
    with open(cache+'/'+fname.replace('.pkl', '.tmp'), 'wb') as f:
        pickle.dump(data_dict, f)
    os.rename(cache+'/'+fname.replace('.pkl', '.tmp'), cache+'/'+fname)
    print("Dumped Successfully: ", fname)
    with open(cache+"/done.txt", "a+") as f:
        f.write("{}".format(datetime.datetime.now())+fname+'\n')
    return data_dict


def get_futures_dataframes(symbol_list, date):
    # Retrieve data from MongoDB
    # Merge future 1 and future 2 OI
    # Returm dataframe wits ts as index and oi,ltp as columns

    pd.set_option('mode.chained_assignment', None)
    col = db[date_to_connectionname(date, "FUT")]
    future_months = [1, 2]
    # Get filtered data
    x = col.find({'sym': {"$in": symbol_list}, 'f_m': {"$in": future_months}}, {
                 'sym': 1, 'f_m': 1, 'ltp': 1, 'oi': 1, 'ts': 1, 'ltq': 1, '_id': 0})
    df = pd.DataFrame(list(x))
    fut_dfs = {}
    for symbol in list(set(df['sym'].to_list())):
        df_symbol = df[df['sym'] == symbol]
        df1 = df_symbol[df['f_m'] == 1]
        df2 = df_symbol[df['f_m'] == 2]
        df1.set_index('ts', inplace=True, drop=True)
        df2.set_index('ts', inplace=True, drop=True)
        df1.sort_index(inplace=True)
        df2.sort_index(inplace=True)
        # merge on ts
        df_merged = df1.merge(
            df2, how='outer', left_index=True, right_index=True)
        # Removing first nan values from df_merged
        df_merged = df_merged[(df_merged['oi_x'] > 0).to_list().index(1):]
        # Fill Empty oi_y values with previous oi_y value
        df_merged.fillna(method='ffill', inplace=True)
        # Add ing new column for oi_x + oi_y
        df_merged['oi'] = df_merged['oi_x'] + df_merged['oi_y']
        df_merged['ltp'] = df_merged['ltp_x']
        df_merged['qty'] = df_merged['ltq_x']
        df_merged = df_merged[['oi', 'ltp']]
        fut_dfs[symbol] = df_merged
    return fut_dfs


# Do we need these variables?



def get_expiry_dates_dict(expiry_dates,instrument,month, year):
    return {
        'instrument': instrument,
        'month': month,
        'year': year,
        'expiry_dates': expiry_dates
    }

def parse_month(month):
    if month == 'JAN':
        return 1
    elif month == 'FEB':
        return 2
    elif month == 'MAR':
        return 3
    elif month == 'APR':
        return 4
    elif month == 'MAY':
        return 5
    elif month == 'JUN':
        return 6
    elif month == 'JUL':
        return 7
    elif month == 'AUG':
        return 8
    elif month == 'SEP':
        return 9
    elif month == 'OCT':
        return 10
    elif month == 'NOV':
        return 11
    elif month == 'DEC':
        return 12
    else:
        return 0

def get_expiry_dates_from_mongodb(month, year, instrument):
    year = int(year)
    if isinstance(month, str):
        month = parse_month(month)
    mongoconn = pymongo.MongoClient()
    expiry_db  = mongoconn['expiry_db']['expiry_db']
    expiry_dates  = expiry_db.find({'instrument': instrument, 'month': month, 'year': year})
    expiry_dates = [x['expiry_dates'] for x in expiry_dates]
    if len(expiry_dates):
        expiry_dates = expiry_dates[0]
    else:
        return []
    mongoconn.close()
    return expiry_dates


if __name__ == '__main__':
# symbol_list = pd.read_csv("data.csv")['symbols'].to_list()
# expiry_db = client['expiry_db']['expiry_db']
    instrument = 'NIFTY'
    # t1 = time.time()
    expiry_dates = set()
    for year in range(2019, 2022):
        for month in range(1, 13):
            if year == 2019 and month == 1:
                continue
            if year == 2021 and month == 12:
                continue
            for expiry_date in get_expiry_dates_from_mongodb(month, year, 'NIFTY'):
                if expiry_date < datetime.datetime.now():
                    expiry_dates.add(expiry_date)
    # Change this later
    num_processes = 8
    expiry_dates = list(expiry_dates)
    # print(list(zip(expiry_dates, repeat(instrument))))
    start = time.time()
    with Pool(num_processes) as pool:
        result = pool.starmap(get_options_dictionary, zip(repeat(instrument), expiry_dates))
    end = time.time()
    with open('timing.txt', 'a+') as f:
        f.write(f'{start} + {end - start}\n')



# for expiry in expiry_dates:
#     start = time.time()
#     print("Running for ", expiry, " expiries")
#     try:
#         ret_dict = get_options_dictionary(instrument, expiry)
#     except:
#         print("Error in getting options data for ", expiry)
#         continue
#     end = time.time()
#     print("Time taken to get options dictionary: ", end-start)
#     # break


