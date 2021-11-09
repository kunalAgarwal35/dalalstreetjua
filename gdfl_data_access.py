import datetime
import os
import pickle

import pandas as pd

location = 'E:/Shared_Trading/tickdata'
excluded_folders = ['JAN_2019']
os.listdir(location)


def load_instruments():
    global location
    allins = []
    for monthyear in os.listdir(location):
        dates = os.listdir(os.path.join(location, monthyear))
        for date in dates:
            for csv in os.listdir(os.path.join(location, monthyear, date, 'Futures', '-I')):
                csv = csv.replace('-I.NFO.csv', '')
                if csv not in allins:
                    allins.append(csv)

    return allins


def csv_breakdown_opt(csv):
    try:
        csv = csv.replace('.NFO.csv', '')
    except:
        pass
    ncsv = list(csv)
    instrument = ''
    for item in ncsv:
        try:
            item = int(item)
            break
        except:
            instrument += item
    csv = csv.replace(instrument, '')
    exp = csv[:7]
    # try:
    #     # expiry = datetime.datetime.strptime(exp,'%d%b%Y').date()
    # except:
    expiry = datetime.datetime.strptime(exp, '%d%b%y').date()
    # expiry = dateutil.parser.parse(csv[:7]).date()
    opt_name = csv[7:]
    breakdown = {'instrument': instrument, 'expiry': expiry, 'option_name': opt_name}
    return breakdown


def rename_to_clean(dir):
    folders = os.listdir(dir)
    if 'Option' in folders:
        os.rename(os.path.join(dir, 'Option'), os.path.join(dir, 'Options'))


def find_expiries(instrument):
    global location, excluded_folders
    expiries = []
    for monthyear in os.listdir(location):
        if monthyear in excluded_folders:
            continue
        dates = os.listdir(os.path.join(location, monthyear))
        date = dates[1]
        for csv in os.listdir(os.path.join(location, monthyear, date, 'Options')):
            if 'TV18BRDCST' in csv:
                continue
            try:
                bd = csv_breakdown_opt(csv)
            except Exception as e:
                print(os.path.join(location, monthyear, date, 'Options', csv), e)
            if bd['instrument'] == instrument:
                if bd['expiry'] not in expiries:
                    if bd['expiry'].year > 2021 or bd['expiry'].year < 2019:
                        print(os.path.join(location, monthyear, date, 'Options', csv), bd['expiry'])
                    expiries.append(bd['expiry'])
    return expiries


instruments = load_instruments()


def get_opt_ltp_df(instrument, expiry, from_date):
    global location, excluded_folders
    if instrument not in instruments:
        print('Instrument not found')
        return {}
    dflist = {}
    for monthyear in os.listdir(location):
        print(monthyear)
        if monthyear in excluded_folders:
            continue
        dates = os.listdir(os.path.join(location, monthyear))
        dates.sort()
        for date in dates:
            dtdate = datetime.datetime.strptime(date[-8:], '%d%m%Y').date()
            if dtdate < from_date or dtdate > expiry:
                continue
            rename_to_clean(os.path.join(location, monthyear, date))
            for csv in os.listdir(os.path.join(location, monthyear, date, 'Options')):
                if 'TV18BRDCST' in csv:
                    continue
                try:
                    bd = csv_breakdown_opt(csv)
                except Exception as e:
                    print(os.path.join(location, monthyear, date, 'Options', csv), e)
                if bd['instrument'] == instrument and bd['expiry'] == expiry:
                    print(os.path.join(location, monthyear, date, 'Options', csv), bd['expiry'])
                    if csv.replace('.NFO.csv', '') not in dflist.keys():
                        dflist[csv.replace('.NFO.csv', '')] = pd.read_csv(
                            os.path.join(location, monthyear, date, 'Options', csv))
                    else:
                        dflist[csv.replace('.NFO.csv', '')] = pd.concat([dflist[csv.replace('.NFO.csv', '')],
                                                                         pd.read_csv(
                                                                             os.path.join(location, monthyear, date,
                                                                                          'Options', csv))],
                                                                        ignore_index=True)

    return dflist


instrument = 'NIFTY'


def opt_ltps(instrument, expiry):
    expiries = find_expiries(instrument)
    expiries.sort()
    from_date = expiries[expiries.index(expiry) - 1]
    dflist = get_opt_ltp_df(instrument, expiry, from_date)
    instruments = list(dflist.keys())
    label = instrument + expiry.strftime('%m%b%y') + 'opt_ltps' + '.pickle'
    if label in os.listdir('cache'):
        [ltps, bidasks, oi] = pickle.load(open('cache/' + label, 'rb'))
        return ltps, bidasks, oi
    ltps, bidasks, oi = {}, {}, {}
    for ins in instruments:
        print(instruments.index(ins) / len(instruments))
        df = dflist[ins]
        datelist = df['Date'].to_list()
        timelist = df['Time'].to_list()
        dtformat = '%d/%m/%Y %H:%M:%S'
        dtlist = [datetime.datetime.strptime(datelist[i] + ' ' + timelist[i], dtformat) for i in
                  range(0, len(datelist))]
        for timestamp in dtlist:
            timestampk = timestamp - datetime.timedelta(seconds=timestamp.second % 5)
            if timestampk not in list(ltps.keys()):
                print(timestampk)
                ltps[timestampk] = {}
                bidasks[timestampk] = {}
                oi[timestampk] = {}
            n = dtlist.index(timestamp)
            ltps[timestampk][csv_breakdown_opt(ins)['option_name']] = (df['BuyPrice'].tolist()[n] +
                                                                       df['SellPrice'].tolist()[n]) / 2
            bidasks[timestampk][csv_breakdown_opt(ins)['option_name']] = (
                        df['SellPrice'].tolist()[n] - df['BuyPrice'].tolist()[n])
            oi[timestampk][csv_breakdown_opt(ins)['option_name']] = df['OpenInterest'].tolist()[n]
    pickle.dump([ltps, bidasks, oi], open('cache/' + label, 'wb'))
    return ltps, bidasks, oi


expiries = find_expiries(instrument)
expiries.sort()
for expiry in expiries[100:]:
    print(expiry)
    if datetime.date(2019, 2, 1) < expiry < datetime.datetime.now().date():
        ltps, bidasks, oi = opt_ltps(instrument, expiry)

#
# for timestamp in ltps.keys():
#     print(timestamp,len(ltps[timestamp]))
# for key in dflist.keys():
#     print(key,len(dflist[key]))
