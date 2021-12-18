import pickle
import os
import time
import pandas as pd
import numpy as np
import function_store2 as fs

expiry_loc = 'mongo_cache'
expiry_files = os.listdir(expiry_loc)

def get_pickle_data(file_name):
    with open(os.path.join(expiry_loc, file_name), 'rb') as f:
        return pickle.load(f)

def test_call_sell(file_name):
    data_dict = get_pickle_data(file_name)
    if len(data_dict):
        timestamps = data_dict.keys()
        for timestamp in timestamps:
            pb = data_dict[timestamp]
            ltps = pb['ltps']
            ois = pb['ois']
            bidasks = pb['bidasks']
            print(ltps,ois, bidasks)
            breakpoint()

