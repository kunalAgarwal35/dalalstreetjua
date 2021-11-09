import pickle
import os
import datetime
import numpy as np
import pandas as pd
import mpl_charts as mc
oldticksdict={}
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('oldticks')]

with open('April_kite.instruments', 'rb') as handle:
    instruments = (pickle.load(handle))
handle.close()

def ibat(x):
    for ins in instruments:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def loadticks(wait_min):
    global oldticksdict
    print("Loading Ticks")
    for date in dates_list:
        if date.hour == 9 and date.minute < wait_min:
            # print(date)
            try:
                with open('oldticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
def loadcustomticks(from_time,to_time):
    global oldticksdict
    print("Loading Ticks")
    for date in dates_list:
        if from_time<date<to_time:
            # print(date)
            try:
                with open('oldticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                oldticksdict[date] = {}
                for token in tick.keys():
                    oldticksdict[date][token] = tick[token]
            except:
                continue
topbuy={}
topsell={}
avgbuymoney = {}
avgsellmoney = {}
ltp_open = {}
ltp_920 = {}
ltp_close = {}
tradelist = {}
dfs={}
def find_results(top_x,comp_min,wait_min,trade_end_hour,trade_end_min,movemaxthres,moveminthres,bs_long,bs_short,savechart,short_historical_drop_thres,long_historical_drop_thres,historical_days):
    global oldticksdict
    topbuy={}
    topsell={}
    avgbuymoney0 = {}
    avgsellmoney0 = {}
    avgbuymoney1 = {}
    avgsellmoney1 = {}
    dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('oldticks')]
    oldticksdict={}
    loadticks(wait_min)
    for item in oldticksdict.keys():
        tick=oldticksdict[item]
        date=item.date()
        if date not in avgbuymoney0.keys():
            avgbuymoney0[date]={}
            avgsellmoney0[date]={}
        for token in tick.keys():
            buy_money=tick[token]['buy_quantity']*tick[token]['last_price']/100000000
            sell_money=tick[token]['sell_quantity']*tick[token]['last_price']/100000000
            if token not in avgsellmoney0[date].keys():
                avgsellmoney0[date][token]=[sell_money]
            else:
                templist=avgsellmoney0[date][token]
                templist.append(buy_money)
                avgsellmoney0[date][token]=templist
            if token not in avgbuymoney0[date].keys():
                avgbuymoney0[date][token]=[buy_money]
            else:
                templist = avgbuymoney0[date][token]
                templist.append(sell_money)
                avgbuymoney0[date][token] = templist
    oldticksdict = {}
    loadticks(comp_min)
    for item in oldticksdict.keys():
        tick = oldticksdict[item]
        date = item.date()
        if date not in avgbuymoney1.keys():
            avgbuymoney1[date] = {}
            avgsellmoney1[date] = {}
        for token in tick.keys():
            buy_money = tick[token]['buy_quantity'] * tick[token]['last_price'] / 100000000
            sell_money = tick[token]['sell_quantity'] * tick[token]['last_price'] / 100000000
            if token not in avgsellmoney1[date].keys():
                avgsellmoney1[date][token] = [sell_money]
            else:
                templist = avgsellmoney1[date][token]
                templist.append(buy_money)
                avgsellmoney1[date][token] = templist
            if token not in avgbuymoney1[date].keys():
                avgbuymoney1[date][token] = [buy_money]
            else:
                templist = avgbuymoney1[date][token]
                templist.append(sell_money)
                avgbuymoney1[date][token] = templist

    for date in avgbuymoney0.keys():
        for token in avgbuymoney0[date].keys():
            avgbuymoney0[date][token]=np.mean(avgbuymoney0[date][token])
    for date in avgsellmoney0.keys():
        for token in avgsellmoney0[date].keys():
            avgsellmoney0[date][token] = np.mean(avgsellmoney0[date][token])
    for date in avgbuymoney1.keys():
        for token in avgbuymoney1[date].keys():
            avgbuymoney1[date][token]=np.mean(avgbuymoney1[date][token])
    for date in avgsellmoney1.keys():
        for token in avgsellmoney1[date].keys():
            avgsellmoney1[date][token] = np.mean(avgsellmoney1[date][token])


    for date in avgbuymoney0.keys():
        topbuy[date]=sorted(avgbuymoney0[date], key=avgbuymoney0[date].get, reverse=True)[:top_x]
        topsell[date]=sorted(avgsellmoney0[date], key=avgsellmoney0[date].get, reverse=True)[:top_x]

    ltp_open={}
    ltp_920={}
    ltp_close={}
    tradelist={}
    for date in topbuy.keys():
        tradelist[date]=list(set([*topbuy[date],*topsell[date]]))
        ltp_open[date]={}
        ltp_920[date]={}
        ltp_close[date]={}

    for item in dates_list:
        date=item.date()
        if item.hour == 9 and item.minute <16 and item.second <10:
            # print(item)
            try:
                with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_open[date].keys():
                        ltp_open[date][token]=tick[token]['last_price']
            except:
                pass
        if item.hour == 9 and item.minute == wait_min and item.second < 10:
            # print(item)
            try:
                with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_920[date].keys():
                        ltp_920[date][token] = tick[token]['last_price']
            except:
                pass
        if item.hour == trade_end_hour and item.minute == trade_end_min and item.second < 10:
            # print(item)
            try:
                with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_close[date].keys():
                        ltp_close[date][token] = tick[token]['last_price']
            except:
                pass

    df=pd.DataFrame(columns=['Date','Token','First 15','Rest of the day','Result','Avg Buy Money','Avg Sell Money','B/S'])

    for date in tradelist.keys():
        print('Trades on ',date,'>>>',len(tradelist[date]))
        print(tradelist[date])
        for token in tradelist[date]:
            try:
                newrow={'Token':ibat(token),'First 15':100*np.log(ltp_920[date][token]/ltp_open[date][token]),
                        'Date':date.strftime("%m%d%Y"),
                        'Rest of the day':100*np.log(ltp_close[date][token]/ltp_920[date][token]),
                        'Avg Buy Money':avgbuymoney0[date][token],
                        'Avg Sell Money':avgsellmoney0[date][token],
                        'B/S':avgbuymoney0[date][token]/avgsellmoney0[date][token],
                        'B/S(old)':avgbuymoney1[date][token]/avgsellmoney1[date][token]}
                if newrow['First 15']*newrow['Rest of the day']>0:
                    newrow['Result']=abs(newrow['Rest of the day'])
                else:
                    newrow['Result']=abs(newrow['Rest of the day'])*-1
                if (newrow['B/S']>bs_long and newrow['First 15']>0) or (newrow['B/S']<bs_short and newrow['First 15']<0):
                    if abs(newrow['First 15'])<movemaxthres and abs(newrow['First 15'])>moveminthres:
                        days30 = datetime.timedelta(days=30)
                        days1 = datetime.timedelta(days=1)
                        last_n_days=mc.historical(token,(date-days30).strftime("%Y-%m-%d"),date-days1,'day',0)
                        last_n_days=last_n_days[-1*historical_days:]
                        last_n_days=100*np.log(last_n_days['close'].to_list()[len(last_n_days)-1]/last_n_days['open'].to_list()[0])
                        if newrow['First 15'] > 0:
                            direction = 'Long'
                        else:
                            direction = 'Short'
                        # print(direction,newrow['Rest of the day'])
                        if (last_n_days>long_historical_drop_thres and direction=='Long' and newrow['B/S']/newrow['B/S(old)']>1) or (last_n_days<short_historical_drop_thres and direction=='Short' and newrow['B/S']/newrow['B/S(old)']<1):
                            print(date,token,newrow['B/S'],last_n_days,direction,newrow['B/S']/newrow['B/S(old)'])
                            df=df.append(newrow, ignore_index=True)
                            if savechart:
                                fdate = date.strftime("%Y-%m-%d") + ' 9:15:00'
                                tdate = date.strftime("%Y-%m-%d") + ' 15:30:00'
                                interval = '5minute'
                                o_i = 0
                                if type(newrow['Token'])==str:
                                    name = newrow['Token']
                                else:
                                    name = str(newrow['Token'])
                                filepath = 'ladder trades/charts/' + date.strftime("%Y-%m-%d")+' '+name+' '+direction
                                # print('Saving Chart for ',name)
                                # name, filepath, token, fdate, tdate, interval, o_i='TITAN21APRFUT', 'ladder trades/charts/2021-04-26 TITAN21APRFUT Short', 16629250, '2021-04-26 9:19:00', '2021-04-26 15:15:00', '5minute', 0
                                mc.savechart(name, filepath, token, fdate, tdate, interval, o_i)
            except:
                print(date,token)
    df.to_csv('ladder trades/Results.csv',index=False)

list_of_tick_delta=[100,40,10]
def find_results_bs_strat(list_of_tick_delta,net_tres,stop_pct,date_to_run):
    global oldticksdict
    secs=0
    for sec in list_of_tick_delta:
        secs=secs+sec
    start_time=datetime.time(9,15,1)
    secs=datetime.timedelta(seconds=secs)
    end_time=(datetime.datetime.combine(datetime.date(1,1,1),start_time)+secs).time()
    dates_list=[]
    for item in os.listdir('oldticks'):
        dtobject=datetime.datetime.strptime(item, "%m%d%Y-%H%M%S")
        if dtobject.date()==date_to_run:
            dates_list.append(dtobject)

    oldticksdict={}

    loadcustomticks(datetime.datetime.combine(date_to_run,start_time),datetime.datetime.combine(date_to_run,datetime.time(15,30,00)))
    for item in oldticksdict.keys():
        if item.time()<end_time:
            continue
        tick=oldticksdict[item]
        for token in tick.keys():
            buy_money=tick[token]['buy_quantity']*tick[token]['last_price']/100000000
            sell_money=tick[token]['sell_quantity']*tick[token]['last_price']/100000000
            if token not in avgsellmoney0[date].keys():
                avgsellmoney0[date][token]=[sell_money]
            else:
                templist=avgsellmoney0[date][token]
                templist.append(buy_money)
                avgsellmoney0[date][token]=templist
            if token not in avgbuymoney0[date].keys():
                avgbuymoney0[date][token]=[buy_money]
            else:
                templist = avgbuymoney0[date][token]
                templist.append(sell_money)
                avgbuymoney0[date][token] = templist
    oldticksdict = {}
    loadticks(comp_min)
    for item in oldticksdict.keys():
        tick = oldticksdict[item]
        date = item.date()
        if date not in avgbuymoney1.keys():
            avgbuymoney1[date] = {}
            avgsellmoney1[date] = {}
        for token in tick.keys():
            buy_money = tick[token]['buy_quantity'] * tick[token]['last_price'] / 100000000
            sell_money = tick[token]['sell_quantity'] * tick[token]['last_price'] / 100000000
            if token not in avgsellmoney1[date].keys():
                avgsellmoney1[date][token] = [sell_money]
            else:
                templist = avgsellmoney1[date][token]
                templist.append(buy_money)
                avgsellmoney1[date][token] = templist
            if token not in avgbuymoney1[date].keys():
                avgbuymoney1[date][token] = [buy_money]
            else:
                templist = avgbuymoney1[date][token]
                templist.append(sell_money)
                avgbuymoney1[date][token] = templist

    for date in avgbuymoney0.keys():
        for token in avgbuymoney0[date].keys():
            avgbuymoney0[date][token]=np.mean(avgbuymoney0[date][token])
    for date in avgsellmoney0.keys():
        for token in avgsellmoney0[date].keys():
            avgsellmoney0[date][token] = np.mean(avgsellmoney0[date][token])
    for date in avgbuymoney1.keys():
        for token in avgbuymoney1[date].keys():
            avgbuymoney1[date][token]=np.mean(avgbuymoney1[date][token])
    for date in avgsellmoney1.keys():
        for token in avgsellmoney1[date].keys():
            avgsellmoney1[date][token] = np.mean(avgsellmoney1[date][token])


    for date in avgbuymoney0.keys():
        topbuy[date]=sorted(avgbuymoney0[date], key=avgbuymoney0[date].get, reverse=True)[:top_x]
        topsell[date]=sorted(avgsellmoney0[date], key=avgsellmoney0[date].get, reverse=True)[:top_x]

    ltp_open={}
    ltp_920={}
    ltp_close={}
    tradelist={}
    for date in topbuy.keys():
        tradelist[date]=list(set([*topbuy[date],*topsell[date]]))
        ltp_open[date]={}
        ltp_920[date]={}
        ltp_close[date]={}

    for item in dates_list:
        date=item.date()
        if item.hour == 9 and item.minute <16 and item.second <10:
            # print(item)
            try:
                with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_open[date].keys():
                        ltp_open[date][token]=tick[token]['last_price']
            except:
                pass
        if item.hour == 9 and item.minute == wait_min and item.second < 10:
            # print(item)
            try:
                with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_920[date].keys():
                        ltp_920[date][token] = tick[token]['last_price']
            except:
                pass
        if item.hour == trade_end_hour and item.minute == trade_end_min and item.second < 10:
            # print(item)
            try:
                with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick = (pickle.load(handle))
                handle.close()
                for token in tick.keys():
                    if token not in ltp_close[date].keys():
                        ltp_close[date][token] = tick[token]['last_price']
            except:
                pass

    df=pd.DataFrame(columns=['Date','Token','First 15','Rest of the day','Result','Avg Buy Money','Avg Sell Money','B/S'])

    for date in tradelist.keys():
        print('Trades on ',date,'>>>',len(tradelist[date]))
        print(tradelist[date])
        for token in tradelist[date]:
            try:
                newrow={'Token':ibat(token),'First 15':100*np.log(ltp_920[date][token]/ltp_open[date][token]),
                        'Date':date.strftime("%m%d%Y"),
                        'Rest of the day':100*np.log(ltp_close[date][token]/ltp_920[date][token]),
                        'Avg Buy Money':avgbuymoney0[date][token],
                        'Avg Sell Money':avgsellmoney0[date][token],
                        'B/S':avgbuymoney0[date][token]/avgsellmoney0[date][token],
                        'B/S(old)':avgbuymoney1[date][token]/avgsellmoney1[date][token]}
                if newrow['First 15']*newrow['Rest of the day']>0:
                    newrow['Result']=abs(newrow['Rest of the day'])
                else:
                    newrow['Result']=abs(newrow['Rest of the day'])*-1
                if (newrow['B/S']>bs_long and newrow['First 15']>0) or (newrow['B/S']<bs_short and newrow['First 15']<0):
                    if abs(newrow['First 15'])<movemaxthres and abs(newrow['First 15'])>moveminthres:
                        days30 = datetime.timedelta(days=30)
                        days1 = datetime.timedelta(days=1)
                        last_n_days=mc.historical(token,(date-days30).strftime("%Y-%m-%d"),date-days1,'day',0)
                        last_n_days=last_n_days[-1*historical_days:]
                        last_n_days=100*np.log(last_n_days['close'].to_list()[len(last_n_days)-1]/last_n_days['open'].to_list()[0])
                        if newrow['First 15'] > 0:
                            direction = 'Long'
                        else:
                            direction = 'Short'
                        # print(direction,newrow['Rest of the day'])
                        if (last_n_days>long_historical_drop_thres and direction=='Long' and newrow['B/S']/newrow['B/S(old)']>1) or (last_n_days<short_historical_drop_thres and direction=='Short' and newrow['B/S']/newrow['B/S(old)']<1):
                            print(date,token,newrow['B/S'],last_n_days,direction,newrow['B/S']/newrow['B/S(old)'])
                            df=df.append(newrow, ignore_index=True)
                            if savechart:
                                fdate = date.strftime("%Y-%m-%d") + ' 9:15:00'
                                tdate = date.strftime("%Y-%m-%d") + ' 15:30:00'
                                interval = '5minute'
                                o_i = 0
                                if type(newrow['Token'])==str:
                                    name = newrow['Token']
                                else:
                                    name = str(newrow['Token'])
                                filepath = 'ladder trades/charts/' + date.strftime("%Y-%m-%d")+' '+name+' '+direction
                                # print('Saving Chart for ',name)
                                # name, filepath, token, fdate, tdate, interval, o_i='TITAN21APRFUT', 'ladder trades/charts/2021-04-26 TITAN21APRFUT Short', 16629250, '2021-04-26 9:19:00', '2021-04-26 15:15:00', '5minute', 0
                                mc.savechart(name, filepath, token, fdate, tdate, interval, o_i)
            except:
                print(date,token)
    df.to_csv('ladder trades/Results.csv',index=False)



top_x=50
comp_min=17
wait_min=19
trade_end_hour=15
trade_end_min=15
movemaxthres=1.5
moveminthres=0.2
bs_long=1
bs_short=1
savechart=1
short_historical_drop_thres=1
long_historical_drop_thres=-1
historical_days=2
find_results(top_x,comp_min,wait_min,trade_end_hour,trade_end_min,movemaxthres,moveminthres,bs_long,bs_short,savechart,long_historical_drop_thres,short_historical_drop_thres,historical_days)

# tc=[16492802, 16492034, 16498946, 16568322, 16617218, 16617474, 16493058, 16619522, 16563970, 16593666, 16498434, 16625666, 16560130, 16568578, 16625922, 16560386, 16495106, 16560642, 16569090, 16560898, 16499714, 16495618, 16561410, 16500226, 16594434, 16496386, 16568066, 16574210, 16570114, 16561922, 16562178, 16570370, 16594946, 16492546, 16500738, 16616962, 16580354, 16504834, 16500994, 16578818, 16562434, 16570626, 16644354, 16595458, 16644610, 16566786, 16494338, 16644866, 16575234, 16567042, 15706882, 16645634, 16563202, 16575490, 16497666, 16645122, 16567554, 16645378, 16493826, 16592386]
# lm=[16492802, 16492034, 16498946, 16568322, 16617218, 16617474, 16493058, 16619522, 16563970, 16593666, 16498434, 16625666, 16560130, 16568578, 16625922, 16560386, 16495106, 16560642, 16569090, 16560898, 16499714, 16495618, 16561410, 16500226, 16594434, 16496386, 16568066, 16574210, 16570114, 16561922, 16562178, 16570370, 16594946, 16492546, 16500738, 16616962, 16580354, 16504834, 16500994, 16578818, 16562434, 16570626, 16644354, 16595458, 16644610, 16566786, 16494338, 16644866, 16575234, 16567042, 15706882, 16645634, 16563202, 16575490, 16497666, 16645122, 16567554, 16645378, 16493826, 16592386]
#
# len(list(set([*tc,*lm])))
# len(tc)
# for element in tc:
#     if element not in lm:
#         print(element)
# print('---')
# for element in lm:
#     if element not in tc:
#         print(element)