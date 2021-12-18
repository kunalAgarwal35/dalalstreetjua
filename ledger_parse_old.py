import datetime
import pandas as pd
import os
import pickle

ledgers = 'one_legged_ledgers'

def import_and_concatenate_ledgers(ledger_dir):
    """
    Import all the ledger files in a directory, concatenate them, and return a dataframe.
    """
    df_list = []
    for file in os.listdir(ledger_dir):
        df = pickle.load(open(os.path.join(ledger_dir, file), 'rb'))
        print(file)
        get_net_returns(df)
        df_list.append(df)
    df = pd.concat(df_list)
    df.sort_values(by=['Timestamp'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df
def get_net_returns(df):
    df['value'] = -df['Price']*df['Qty']
    print(df.shape)
    print('Returns before Brokerage: ', df['value'].sum())
    brokerage = df['Brokerage'].sum()
    print('Brokerage: ', brokerage)
    print('Returns after Brokerage: ', df['value'].sum()-brokerage)
    print('--------------------------------')

def get_historical_ltps(ledgers,cache):
    """
    Get the historical LTPs of all the stocks in the ledger files.
    """
    df = import_and_concatenate_ledgers(ledgers)
    unique_instruments = df['Instrument'].unique()
    unique_calls = [i for i in unique_instruments if'CE' in i]
    unique_puts = [i for i in unique_instruments if'PE' in i]
    expiries = list(set([datetime.datetime.strptime(i.split('CE')[1],'%Y-%m-%d') for i in unique_calls]))
    expiries.extend(list(set([datetime.datetime.strptime(i.split('PE')[1],'%Y-%m-%d') for i in unique_puts])))
    expiries = list(set(expiries))
    expiries.sort()
    # for mongo_cache
    expiry_files = [i.strftime('NIFTY_%d%b%Y.pkl') for i in expiries]
    # for ev_backtest_data
    expiry_files = [i.strftime('NIFTY_%d%b%Y_optltps.pickle') for i in expiries]
    expiries = dict(zip(expiries, expiry_files))
    ltp_parent_dict = {}
    for expiry in expiries.keys():
        print(expiry)
        file = expiries[expiry]
        pb = pickle.load(open(os.path.join(cache, file), 'rb'))
        for timestamp in pb.keys():
            # drop all keys except ltps and bidasks from pb[timestamp]
            pb[timestamp] = {'ltps':pb[timestamp]['ltps'], 'bidasks':pb[timestamp]['bidasks']}
        ltp_parent_dict[expiry] = pb
    return ltp_parent_dict








def check():
    global ledgers
    df = import_and_concatenate_ledgers(ledgers)
    get_net_returns(df)


check()



with open("one_legged_ledgers_hd/ledgerNIFTY_28Feb2019.pkl", "rb") as f:
    df = pickle.load(f)

print(df.shape)