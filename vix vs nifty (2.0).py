import pandas as pd
import time
import datetime
import zd
import numpy as np
import os
import pickle
import xlwings as xw


file_path='xlwings.xlsx'
s=xw.Book(file_path).sheets['Sheet3']
allins = zd.kite.instruments()
market_open = datetime.time(9,15,00)
market_close = datetime.time(15,30,00)
curr_ins = {}
iupac="%m%d%Y-%H%M"
for ins in allins:
    curr_ins[ins['instrument_token']] = ins
types=[]
segments=[]
exchanges=[]
ticksizes=[]
indices=[]
last_tick={}
ltp_by_symbol={}

for ins in curr_ins.keys():
    if curr_ins[ins]['instrument_type'] not in types:
        types.append(curr_ins[ins]['instrument_type'])
    if curr_ins[ins]['segment'] not in segments:
        segments.append(curr_ins[ins]['segment'])
    if curr_ins[ins]['exchange'] not in exchanges:
        exchanges.append(curr_ins[ins]['exchange'])
    if curr_ins[ins]['tick_size'] not in ticksizes:
        ticksizes.append(curr_ins[ins]['tick_size'])
    if curr_ins[ins]['segment'] == 'INDICES':
        indices.append(curr_ins[ins])

for index in indices:
    if index['name']=='INDIA VIX':
        # print(index)
        vix_instrument=index
        break
for index in indices:
    if index['name']=='NIFTY 50':
        # print(index)
        nifty_50_instrument=index
        break

def get_ltps():
    fname = os.listdir('optionticks')
    fname = fname[len(fname)-1]
    return pickle.load(open('optionticks/'+fname,"rb"))

def get_historical(instrument_token,fdate,tdate,interv):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    if filename in os.listdir('get_historical'):
        df = pd.read_csv('get_historical/' + filename)
        df['date'] = df[['date']].apply(pd.to_datetime)
        return df
    if interv == "day" and (tdate-fdate).days > 1500:
        fdates=[fdate]
        newtdate=fdate+day1500
        tdates=[newtdate]

        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day1500)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(zd.kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day70 = datetime.timedelta(days=70)
    day1 = datetime.timedelta(days=1)
    if interv == '5minute':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(zd.kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
fdate=datetime.datetime(2011,1,1)
tdate=datetime.datetime(2020,12,31)
dateformat='%Y-%m-%d'

vix=get_historical(vix_instrument['instrument_token'],fdate,tdate, "5minute")
nifty=get_historical(nifty_50_instrument['instrument_token'],fdate,tdate, "5minute")
vix=vix.loc[vix['date'].isin(nifty['date'])]
nifty=nifty.loc[nifty['date'].isin(vix['date'])]

timeframe=5 #in minutes
def tradables(instru, lstrike, ustrike):
    global timeframe
    filtered = []
    types = ['CE', 'PE', 'FUT']
    expfut = 100
    expopt = 100
    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            days = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT' and days < expfut:
                expfut = days
            elif days < expopt:
                expopt = days

    for ins in instru:
        if ins['name'] == 'NIFTY' and ins['instrument_type'] in types:
            daystoexpiry = (ins['expiry'] - datetime.datetime.now().date()).days
            if ins['instrument_type'] == 'FUT':
                if daystoexpiry == expfut:
                    filtered.append(ins)
            elif daystoexpiry == expopt and ins['strike'] >= lstrike and ins['strike'] <= ustrike:
                filtered.append(ins)

    now=datetime.datetime.now()
    trading_sessions=int(((expopt*375)/timeframe)+min(75,int(12*(datetime.datetime.combine(now.date(),market_close)-now).seconds/3600)))
    return filtered, trading_sessions

def nifty_distribution(vix_min, vix_max, trading_sessions):
    t1=time.time()
    nv=vix.loc[(vix['open']<vix_max) & (vix['open']>vix_min)]
    nnifty = nifty.copy()
    nnifty['ret']=nnifty['close'].shift(int(-trading_sessions))
    nnifty = nnifty[nnifty['ret'].notna()]
    nnifty=nnifty[nnifty['date'].isin(nv['date'].to_list())]
    nnifty.reset_index(drop=True,inplace=True)
    ret_distribution=(100*np.log(nnifty['ret']/nnifty['close'])).dropna()
    # ret_distribution.plot.hist(bins=100,alpha=0.5)
    return ret_distribution

# all_tradables, trading_sessions = tradables(allins,15000,16000)
# nifty_ltp = get_ltps()[nifty_50_instrument['instrument_token']]
# nifty_vix=pd.DataFrame()
# nifty_vix['date'] = vix['date']
# nifty_vix['vix_close'] = vix['close']
# nifty_vix['nifty_close'] = nifty['close']
# nifty_vix['ret'] = nifty['close'].shift(-trading_sessions)
# nifty_vix = nifty_vix[nifty_vix['ret'].notna()]
# nifty_vix['dist'] = nifty_ltp*nifty_vix['ret']/nifty_vix['nifty_close']
# mindist=np.percentile(nifty_vix['dist'],1)
# maxdist=np.percentile(nifty_vix['dist'],99)
# nifty_vix = nifty_vix[nifty_vix.dist > mindist]
# nifty_vix = nifty_vix[nifty_vix.dist < maxdist]
# nifty_vix['dist'].plot.hist(bins=100,alpha=0.5)
option_ltps = {}
options_historical_folder = 'current_options/'
def optionltp(option_prices):
    global option_ltps
    for item in option_prices:
        option_ltps[str(item['strike'])+item['type']] = item['last_price']

def get_option_prices(timestamp):
    fname = timestamp.strftime(iupac)
    if fname in os.listdir('temp'):
        return pickle.load(open('temp/'+fname, 'rb'))
    expiries = [datetime.datetime.strptime(item,dateformat) for item in os.listdir(options_historical_folder)]
    expiries.sort()
    for expiry in expiries:
        if timestamp<=expiry:
            # print('Expiry:', expiry)
            break
    foldername = options_historical_folder+expiry.strftime(dateformat)+'/rec/'
    files = os.listdir(foldername)
    optionslist=[]
    for file in files:
        df = pd.read_csv(foldername + file)
        df['date'] = df[['date']].apply(pd.to_datetime)
        if timestamp in df['date'].to_list():
            index = df['date'].to_list().index(timestamp)
            ltp = df['close'][index]
            if 'PE' in file:
                strike = int(file.replace('PE.csv',''))
                type = 'PE'
            else:
                strike = int(file.replace('CE.csv', ''))
                type = 'CE'
            optionslist.append({'strike':strike,'last_price':ltp,'type':type})
    pickle.dump(optionslist,open('temp/'+fname, "wb"))
    return optionslist

def find_expiry(timestamp):
    expiries = [datetime.datetime.strptime(item,dateformat) for item in os.listdir(options_historical_folder)]
    expiries.sort()
    for expiry in expiries:
        if timestamp<=expiry:
            return expiry
def get_ev(option_prices,spot,vix_now,trading_sessions):
    ndis = nifty_distribution(0.9*vix_now,1.1*vix_now,trading_sessions)
    ndis = [round((1 + item/100) * spot) for item in ndis]
    possible_spots = np.arange(int(min(ndis)),int(max(ndis)),1)
    df=pd.DataFrame()
    df['spot'] = possible_spots
    freq=[]
    for item in df['spot']:
        freq.append(ndis.count(item))
    df['freq'] = freq
    cev={}
    pev={}
    ppop={}
    cpop={}
    for d in option_prices:
        if d['type']=='CE':
            label=str(d['strike'])+d['type']
            df[label] = df['spot'] - d['strike']
            df[label].values[df['spot'] < d['strike']] = 0
            df[label] = df[label] - d['last_price']
            cev[label] = (df[label]*df['freq']).mean()
            cpop = ((df[label]>0)*df['freq']).sum()/df['freq'].sum()
        elif d['type']=='PE':
            label=str(d['strike'])+d['type']
            df[label] = d['strike'] - df['spot']
            df[label].values[df['spot'] > d['strike']] = 0
            df[label] = df[label] - d['last_price']
            pev[label] = (df[label]*df['freq']).mean()
            ppop = ((df[label]>0)*df['freq']).sum()/df['freq'].sum()
        else:
            print('Option Type Not found',d)
    return cev, pev, cpop, ppop


open_positions = {}
ledger = pd.DataFrame()
def manage_positions(cev,pev,percentile):
    global open_positions
    tradelist={}
    if len(open_positions) == 0:
        for key in cev.keys():
            if cev[key] == max(cev.values()):
                open_positions[key] = 1
            if cev[key] == min(cev.values()):
                open_positions[key] = -1
        for key in pev.keys():
            if pev[key] == max(pev.values()):
                open_positions[key] = 1
            if pev[key] == min(pev.values()):
                open_positions[key] = -1
        tradelist = open_positions
        return tradelist
    else:
        call_buy_ev = np.percentile(pd.Series(cev.values()),100 - percentile)
        put_buy_ev = np.percentile(pd.Series(pev.values()), 100 - percentile)
        call_sell_ev = np.percentile(pd.Series(cev.values()), percentile)
        put_sell_ev = np.percentile(pd.Series(pev.values()), percentile)
        tradelist={}
        opkeys=list(open_positions.keys())
        for key in opkeys:
            if key in cev.keys() or key in pev.keys():
                type = key[-2:]
                if type=='CE':
                    if open_positions[key] == 1:
                        if cev[key] < call_buy_ev:
                            if key in tradelist.keys():
                                tradelist[key] -= 1
                            else:
                                tradelist[key] = -1
                            for key2 in cev.keys():
                                if cev[key2] == max(cev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] += 1
                                    else:
                                        tradelist[key2] = 1
                                    continue
                    else:
                        if cev[key] > call_sell_ev:
                            if key in tradelist.keys():
                                tradelist[key] += 1
                            else:
                                tradelist[key] = 1
                            for key2 in cev.keys():
                                if cev[key2] == min(cev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] -= 1
                                    else:
                                        tradelist[key2] = -1
                                    continue
                else:
                    if open_positions[key] == 1:
                        if pev[key] < put_buy_ev:
                            if key in tradelist.keys():
                                tradelist[key] -= 1
                            else:
                                tradelist[key] = -1
                            for key2 in pev.keys():
                                if pev[key2] == max(pev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] += 1
                                    else:
                                        tradelist[key2] = 1
                                    continue
                    else:
                        if pev[key] > put_sell_ev:
                            if key in tradelist.keys():
                                tradelist[key] += 1
                            else:
                                tradelist[key] = 1
                            for key2 in pev.keys():
                                if pev[key2] == min(pev.values()):
                                    if key2 in tradelist.keys():
                                        tradelist[key2] -= 1
                                    else:
                                        tradelist[key2] = -1
                                    continue
        for key in tradelist.keys():
            if key in open_positions.keys():
                open_positions[key] += tradelist[key]
            else:
                open_positions[key] = tradelist[key]
        poplist = []
        for key in open_positions.keys():
            if open_positions[key] ==0:
                poplist.append(key)
        for key in poplist:
            open_positions.pop(key)
        if len(tradelist)>0:
            print('Open Positions: ',open_positions)
            print('Tradelist: ',tradelist)
        return tradelist
    return tradelist

def show_tradelist(tradelist,timestamp,option_prices):
    global ledger, open_positions
    for key in tradelist.keys():
        nr = {'timestamp': timestamp, 'expiry': find_expiry(timestamp).date(), 'instrument': key,
              'qty': tradelist[key] * 75, 'price': option_ltps[key]}
        ledger = ledger.append(nr, ignore_index=True)
    # print(tradelist)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    sqpnl = 0
    for key in open_positions.keys():
        if open_positions[key] == 1:
            sqpnl += option_ltps[key]*75
        else:
            sqpnl -= option_ltps[key]*75
    booked_pnl = {}
    poplist = []
    for contract in print_ledger['instrument']:
        if contract not in booked_pnl.keys():
            if print_ledger[print_ledger['instrument']==contract]['qty'].sum() == 0:
                booked_pnl[contract] = print_ledger[print_ledger['instrument']==contract]['value'].sum()
        else:
            if print_ledger[print_ledger['instrument'] == contract]['qty'].sum() != 0:
               poplist.append(contract)
    for key in poplist:
        booked_pnl.pop(key)
    openpnl = (print_ledger['value'].sum() - sum(booked_pnl.values())) + sqpnl
    s.range('F10').value = booked_pnl
    s.range('H9').value = sum(booked_pnl.values())
    s.range('H2').value = sqpnl
    s.range('H5').value = openpnl
    s.range('Z1').value = print_ledger['value'].sum() + sqpnl
    if len(tradelist)!=0:
        payoff_diagram(option_prices)
        # input("Enter to Continue")
def squareoff_all(option_prices):
    global open_positions
    tradelist = {}
    poplist = []
    for key in open_positions.keys():
        for item in option_prices:
            if str(item['strike'])+item['type'] == key:
                tradelist[key] = -open_positions[key]
                poplist.append(key)
    for key in poplist:
        open_positions.pop(key)
    return tradelist

def filteroptions(option_prices,spot,strikepercent):
    filopts = []
    minstrike = spot*(1-strikepercent)
    maxstrike = spot * (1+strikepercent)
    for item in option_prices:
        if minstrike<item['strike']<maxstrike:
            filopts.append(item)

    return filopts

sim_start_date = datetime.datetime(2021,6,1)
sim_stop_date = datetime.datetime(2021,6,22)
nifty2021 = get_historical(nifty_50_instrument['instrument_token'],sim_start_date,sim_stop_date,'5minute')
vix2021 = get_historical(vix_instrument['instrument_token'],sim_start_date,sim_stop_date,'5minute')
expiries = [datetime.datetime.strptime(item,dateformat) for item in os.listdir(options_historical_folder)]
# for item in nifty2021['date']:
#     print(len(get_option_prices(item)))
def payoff_diagram(option_prices):
    ndis = nifty_distribution(0.9 * vix_now, 1.1 * vix_now, trading_sessions)
    ndis = [round((1 + item / 100) * spot) for item in ndis]
    possible_spots = np.arange(int(min(ndis)), int(max(ndis)), 1)
    df = pd.DataFrame()
    df['spot'] = possible_spots
    freq = []
    for item in df['spot']:
        freq.append(ndis.count(item))
    df['freq'] = freq
    for d in option_prices:
        if d['type'] == 'CE':
            label = str(d['strike']) + d['type']
            df[label] = df['spot'] - d['strike']
            df[label].values[df['spot'] < d['strike']] = 0
            df[label] = df[label] - d['last_price']
        elif d['type'] == 'PE':
            label = str(d['strike']) + d['type']
            df[label] = d['strike'] - df['spot']
            df[label].values[df['spot'] > d['strike']] = 0
            df[label] = df[label] - d['last_price']
        else:
            print('Option Type Not found', d)
    df['payoff'] = 0
    for key in open_positions.keys():
        if key not in df.columns:
            return
        df['payoff'] = df['payoff']+open_positions[key]*75*df[key]
    pdf = df[['spot','payoff']]
    s.range('AB1').value = pdf[pdf['spot']%10==0].set_index('spot',drop=True)




# timestamp = nifty2021['date'][6]
for timestamp in nifty2021['date']:
    # print(timestamp)
    if sum(open_positions.values())!=0:
        break
    # t1=time.time()
    till = nifty2021['date'].to_list().index(timestamp)
    start = max(0, till - 30)
    ohlc = nifty2021.loc[start:till].set_index('date',drop=True)
    s.range('J1').value = ohlc
    percentile = 45
    strike_range_percent = 0.025
    option_prices = get_option_prices(timestamp)
    optionltp(option_prices)
    if len(option_prices) == 0:
        continue
    #
    # t2 = time.time()
    # print('M1: ',t2-t1)


    expopt = np.busday_count(datetime.datetime.now().date(), find_expiry(timestamp).date()) + 1
    trading_sessions = expopt*75 - (timestamp - datetime.datetime.combine(timestamp.date(),datetime.time(9,15,00))).seconds/300
    spot = nifty2021['close'][nifty2021['date'].to_list().index(timestamp)]
    vix_now = vix2021['close'][vix2021['date'].to_list().index(timestamp)]
    tradable_option_prices = filteroptions(option_prices, spot,strike_range_percent)
    if trading_sessions<10:
        if len(open_positions) > 0:
            tradelist = squareoff_all(option_prices)
            if sum(open_positions.values()) != 0:
                break
            show_tradelist(tradelist, timestamp, option_prices)
        else:
            continue
    #
    # t3 = time.time()
    # print('M2: ',t3-t2)
    getevfname = 'getev'+timestamp.strftime(iupac)+str(int(strike_range_percent*1000))
    if getevfname in os.listdir('temp'):
        [cev, pev, cpop, ppop] = pickle.load(open('temp/'+getevfname,'rb'))
    else:
        cev, pev, cpop, ppop=get_ev(tradable_option_prices,spot,vix_now,trading_sessions)
        pickle.dump([cev, pev, cpop, ppop],open('temp/'+getevfname,'wb'))
    s.range('A1').value = cev
    s.range('C1').value = pev

    tradelist = manage_positions(cev,pev,percentile)
    if sum(open_positions.values())!=0:
        break
    show_tradelist(tradelist,timestamp,option_prices)

    # print('M3:',time.time()-t3)


#
# for key in tradelist.keys():
#     if 'PE' in key:
#         print(pev[key])


# for ins in last_tick.keys():
#     print(curr_ins[ins]['tradingsymbol'])