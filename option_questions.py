import pickle
import datetime
import os

rdtd = pickle.load(open('ronit_traindata.pkl','rb'))
#Hardcoded Variables:
dayformat = '%Y-%m-%d'
rdtd_days = 'rdtd_days'
def break_pickle():
    dates = []
    day_dict = {}
    processing = ''
    for timestamp in rdtd.keys():
        fname = timestamp.date().strftime(dayformat)+'.pkl'
        if fname not in dates:
            dates.append(fname)
    for fname in dates:
        print('Processing: ', fname)
        day_dict = {}
        for timestamp in rdtd.keys():
            if timestamp.date().strftime(dayformat)+'.pkl' == fname:
                day_dict[timestamp] = rdtd[timestamp]
        pickle.dump(day_dict, open(os.path.join(rdtd_days, fname), 'wb'))
        print(fname, len(day_dict))

# break_pickle()


timestamp = list(rdtd.keys())[400]
fname = timestamp.date().strftime(dayformat)+'.pkl'
day_dict = pickle.load(open(os.path.join(rdtd_days,fname), 'rb'))
def highest_ev_spreads(timestamp, day_dict):
    if timestamp not in day_dict.keys():
        print('Timestamp not found, Returning 0')
        return 0
    else:
        data = day_dict[timestamp]
        puts = data['put_spread_ev']
        calls = data['call_spread_ev']
        ret_dict = {}
        for key,value in puts.items():
            if value == max(list(puts.values())):
                ret_dict[key] = value
        for key, value in calls.items():
            if value == max(list(calls.values())):
                ret_dict[key] = value

        return ret_dict

def spread_result(timestamp,spread,day_dict):
    if timestamp not in day_dict.keys():
        print('Timestamp not found, Returning 0')
        return 0
    else:
        data = day_dict[timestamp]
        puts = data['put_spread_result']
        calls = data['call_spread_result']
        ret_dict = {}
        for key,value in puts.items():
            if key == spread:
                return  value
        for key,value in calls.items():
            if key == spread:
                return  value

