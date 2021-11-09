import os
import pickle
import datetime
import pandas as pd
import numpy as np
import function_store as fs
import dateutil
import xlwings as xw

file_path = 'xlwings.xlsx'
s = xw.Book(file_path).sheets('Custom_settle')
s.clear()
dayformat = '%y-%m-%d'
options_historical = 'nse_options_historical/'
instruments = os.listdir(options_historical)
prob_range = 0.08
trade_range = 0.03
vix_range_percent = 0.2
instrument = 'BANKNIFTY'

expiries = [datetime.datetime.strptime(i,dayformat) for i in os.listdir(options_historical+instrument)]





timestamps = []
expiry_spots = {}
# print('Loading Timestamps')
# for expiry in expiries:
#     contracts = {}
#     contracts_list = os.listdir(options_historical + instrument + expiry.strftime(dayformat))
#     for contract in contracts_list:
#         contracts[contract.replace('.csv','')] = pd.read_csv(options_historical+instrument+expiry.strftime(dayformat)+'/'+contract)
#     for contract in contracts.values():
#         tslist = contract['Date'].to_list()
#         for item in tslist:
#             if item not in timestamps:
#                 timestamps.append(item)
#     if expiry not in expiry_spots.keys():
#         expiry_spots[expiry] = fs.get_banknifty_close(expiry)
# timestamps = [dateutil.parser.parse(i).date() for i in timestamps]


fname = 'get_historical/2010-01-012021-08-01(260105)day.csv'
df = pd.read_csv(fname)
timestamps = [dateutil.parser.parse(i).date() for i in df['date'].to_list()]

evhist = pd.DataFrame()
for timestamp in timestamps:
    if timestamp < datetime.date(2015,1,1):
        continue
    print(timestamp)
    spot = fs.get_banknifty_close(datetime.datetime(timestamp.year,timestamp.month,timestamp.day))
    l,u = spot*(1-prob_range),spot*(1+prob_range)
    # opt_ltps,opt_ois = fs.update_ltp_oi_ohlc(timestamp,fs.get_contracts(instrument,timestamp))
    opt_ltps, opt_ois = fs.update_ltp_oi_ohlc_settle(timestamp, fs.get_contracts(instrument, timestamp))
    vixnow = fs.get_vix_close(timestamp)
    if not vixnow:
        continue
    mean,sd = fs.get_stats_from_vix(timestamp,vix_range_percent,vixnow,fs.find_expiry(timestamp,options_historical+instrument))
    # ps, cs = fs.spread_evs_custom(opt_ltps, opt_ois, l, u,mean,sd)
    # ps, cs = fs.spread_evs_normal(opt_ltps, opt_ois, l, u)
    ps, cs = fs.spread_evs_custom_new_model(opt_ltps, opt_ois,l,u,mean,sd)
    l, u = spot * (1 - trade_range), spot * (1 + trade_range)
    ps,cs = fs.filter_for_tradable_spreads({},ps,cs,l,u)
    spreadevs = {}
    for key in ps.keys():
        spreadevs[key] = ps[key]
    for key in cs.keys():
        spreadevs[key] = cs[key]
    #finding the max ev spread
    for spread in spreadevs.keys():
        if spreadevs[spread] == max(list(spreadevs.values())):
            break

    real,projected = fs.realized_vs_projected({spread:spreadevs[spread]},fs.get_banknifty_close(fs.find_expiry(timestamp,options_historical+instrument)),opt_ltps)
    nr = {'Timestamp':timestamp,'Max_EV':projected,'Realized':real}
    evhist = evhist.append(nr,ignore_index=True)
    evhist['cum_ev'] = evhist['Max_EV'].cumsum()
    evhist['cum_realized'] = evhist['Realized'].cumsum()
    s.range('A1').value = evhist






