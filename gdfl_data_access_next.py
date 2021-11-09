import pandas as pd
import time
import datetime
import function_store2 as fs
import pickle
import os




def expiry_from_picklename(picklename):
    expiry_date = picklename[picklename.index('_')+1:picklename.index('.')]
    expiry_date = expiry_date[:expiry_date.index('_')]
    expiry_date = datetime.datetime.strptime(expiry_date,'%d%b%Y')
    return expiry_date.date()

def get_comprehensive_dict(expiry_date,opt_data):
    opt_ltps = opt_data[0]
    opt_bidask = opt_data[1]
    opt_oi = opt_data[2]
    backtest_data = {}
    timestamps = list(opt_ltps.keys())
    timestamps.sort()
    totalts = len(timestamps)
    n = 0
    t1 = time.time()
    index_expiry_price = fs.get_nifty_close(expiry_date)
    for timestamp in timestamps:
        print(n*100/totalts,'% Time Taken: ',time.time()-t1)
        n+=1
        try:
            spot = fs.get_nifty_close(timestamp)
            vix = fs.get_vix_close(timestamp)
            vix_range_percent = 0.2
            mean,sd = fs.get_stats_from_vix(timestamp,vix_range_percent,expiry_date)
            backtest_data[timestamp] = {'spot': spot, 'vix': vix, 'index_expiry_price': index_expiry_price, 'mean': mean,
                                        'sd': sd, 'ltps': opt_ltps[timestamp], 'ois': opt_oi[timestamp],
                                        'bidasks': opt_bidask[timestamp]}
        except:
            print('Failed for: ',timestamp)
    return backtest_data



exp_dir = 'cache'
destination_dir = 'ev_backtest_data'
for picklename in os.listdir(exp_dir):
    print(picklename)
    if picklename.replace('pickle','processing') not in os.listdir('temp') and picklename not in os.listdir(destination_dir):
        pickle.dump({},open(os.path.join('temp',picklename.replace('pickle','processing')),'wb'))
        print('Processing')
        fname = os.path.join(exp_dir, picklename)
        opt_data = pickle.load(open(fname, 'rb'))
        expiry_date = expiry_from_picklename(picklename)
        cdata = get_comprehensive_dict(expiry_date, opt_data)
        pickle.dump(cdata, open(destination_dir + '/' + picklename, 'wb'))
        os.remove(os.path.join('temp', picklename.replace('pickle', 'processing')))
    else:
        continue






