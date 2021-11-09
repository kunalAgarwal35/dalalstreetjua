import os
import pickle
import datetime
import pandas as pd
import mplfinance as mpf
import zd
allins=zd.kite.instruments()
def ibt(token):
    for ins in allins:
        if ins['instrument_token']==token:
            return ins['tradingsymbol']
ticklist = os.listdir('oldticks')
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in ticklist]
positions={'long':0,'short':0}
ltps={}
last_long_pos_close={}
last_short_pos_close={}
trades={'long_loss':0,'short_loss':0,'long_win':0,'short_win':0}
def ltpdf(tokens):
    for token in tokens:
        ltps[token]=pd.DataFrame(columns=['date','ltp'])
    for date in dates_list:
        if date.hour == 9 and date.minute == 15 and date.second < 2:
            print(date)
        try:
            with open('oldticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                tick=(pickle.load(handle))
            handle.close()
            for token in tokens:
                ltp=tick[token]['last_price']
                newrow={'date':date,'ltp':ltp}
                ltps[token]=ltps[token].append(newrow, ignore_index=True)
        except:
            pass



def trigger(token,date,price,direction,target,stop):
    if direction=='long':
        if token in last_long_pos_close.keys():
            if last_long_pos_close[token]<date:
                print(positions)
                print([direction, price, date])
                df=ltps[token][ltps[token]['date']>date]
                t=price+target
                s=price-stop
                for i in range(0,len(df)):
                    p=df['ltp'].to_list()[i]
                    if p>t:
                        positions['long']=target+positions['long']
                        last_long_pos_close[token]=df['date'].to_list()[i]
                        trades['long_win']=trades['long_win']+1
                        # saveohlclong(date, token, price, last_long_pos_close[token])
                        return
                    if p<s:
                        positions['long'] = positions['long']-stop
                        last_long_pos_close[token] = df['date'].to_list()[i]
                        trades['long_loss'] = trades['long_loss'] + 1
                        # saveohlclong(date, token, price,last_long_pos_close[token])
                        return
        else:
            print(positions)
            print([direction, price, date])
            # saveohlclong(date, token, price)
            df = ltps[token][ltps[token]['date'] > date]
            t = price + target
            s = price - stop
            for i in range(0, len(df)):
                p = df['ltp'].to_list()[i]
                if p > t:
                    positions['long'] = target + positions['long']
                    last_long_pos_close[token] = df['date'].to_list()[i]
                    trades['long_win'] = trades['long_win'] + 1
                    # saveohlclong(date, token, price, last_long_pos_close[token])
                    return
                if p < s:
                    positions['long'] = positions['long'] - stop
                    last_long_pos_close[token] = df['date'].to_list()[i]
                    trades['long_loss'] = trades['long_loss'] + 1
                    # saveohlclong(date, token, price, last_long_pos_close[token])
                    return
    if direction=='short':
        if token in last_short_pos_close.keys():
            if last_short_pos_close[token]<date:
                print(positions)
                print([direction, price, date])
                df=ltps[token][ltps[token]['date']>date]
                t=price-target
                s=price+stop
                for i in range(0,len(df)):
                    p=df['ltp'].to_list()[i]
                    if p<t:
                        positions['short']=target+positions['short']
                        last_short_pos_close[token]=df['date'].to_list()[i]
                        trades['short_win'] = trades['short_win'] + 1
                        # saveohlcshort(date, token, price, last_short_pos_close[token])
                        return
                    if p>s:
                        positions['short'] = positions['short']-stop
                        last_short_pos_close[token] = df['date'].to_list()[i]
                        trades['short_loss'] = trades['short_loss'] + 1
                        # saveohlcshort(date, token, price, last_short_pos_close[token])
                        return
        else:
            print(positions)
            print([direction, price, date])
            df = ltps[token][ltps[token]['date'] > date]
            t = price - target
            s = price + stop
            for i in range(0, len(df)):
                p = df['ltp'].to_list()[i]
                if p > s:
                    positions['short'] = positions['short'] - stop
                    last_short_pos_close[token] = df['date'].to_list()[i]
                    trades['short_loss'] = trades['short_loss'] + 1
                    # saveohlcshort(date, token, price, last_short_pos_close[token])
                    return
                if p < t:
                    positions['short'] = positions['short'] + target
                    last_short_pos_close[token] = df['date'].to_list()[i]
                    trades['short_win'] = trades['short_win'] + 1
                    # saveohlcshort(date, token, price, last_short_pos_close[token])
                    return

df=pd.DataFrame(columns=['time','buy_quantity','buy_price','buy_orders','sell_quantity','sell_price','sell_orders'])
historical={}
def saveohlclong(date,token,ep,enddate):
    if token not in historical.keys():
        historical[token]=pd.DataFrame(zd.kite.historical_data(instrument_token=token,from_date='2021-04-18',to_date='2021-04-23',interval='minute',oi=1))
        historical[token]['date'] = historical[token]['date'].dt.tz_localize(None)
    onemin = datetime.timedelta(minutes=1)
    name=str(ep)
    plotdf=historical[token].loc[(historical[token]['date'] > (date - onemin))]
    plotdf=plotdf.loc[plotdf['date']<(enddate+onemin)]
    plotdf = plotdf.set_index('date')
    mpf.plot(plotdf, type='candlestick', title=name, show_nontrading=False, volume=True,
             savefig='mpllongs/' + date.strftime("%m%d%Y-%H%M%S") + '.png', style='charles')

def saveohlcshort(date,token,ep,enddate):
    if token not in historical.keys():
        historical[token]=pd.DataFrame(zd.kite.historical_data(instrument_token=token,from_date='2021-04-18',to_date='2021-04-23',interval='minute',oi=1))
        historical[token]['date'] = historical[token]['date'].dt.tz_localize(None)
    onemin = datetime.timedelta(minutes=1)
    name=str(ep)
    plotdf=historical[token].loc[(historical[token]['date'] > (date - onemin))]
    plotdf=plotdf.loc[plotdf['date']<(enddate+onemin)]
    plotdf = plotdf.set_index('date')
    mpf.plot(plotdf, type='candlestick', title=name, show_nontrading=False, volume=True,
             savefig='mplshorts/' + date.strftime("%m%d%Y-%H%M%S") + '.png', style='charles')



def backtest(token,minbid,minask,long_target,long_stop,short_target,short_stop):
    positions['long']=0
    positions['short']=0
    trades['long_loss']= 0
    trades['short_loss']= 0
    trades['long_win']= 0
    trades['short_win']=0
    last_short_pos_close[token]=dates_list[0]
    last_long_pos_close[token]=dates_list[0]

    for date in dates_list:
            if date.hour==9 and date.minute==15 and date.second<2:
                print(date)
            try:
                with open('oldticks/' + date.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                    tick=(pickle.load(handle))
                handle.close()

            except:
                continue
            try:
                itr=tick[token]['depth']
            except:
                print(tick[token])
                continue
            for i in range(0,5):
                try:
                    newrow = {'time': date, 'buy_quantity': itr['buy'][i]['quantity'], 'buy_price': itr['buy'][i]['price'],
                              'buy_orders': itr['buy'][i]['orders'], 'sell_quantity': itr['sell'][0]['quantity'],
                              'sell_price': itr['sell'][0]['price'], 'sell_orders': itr['sell'][0]['orders']}
                    if newrow['buy_quantity']>minbid:
                        # saveohlclong(date,token,newrow['sell_price'])
                        trigger(token,date, newrow['sell_price'], 'long', long_target, long_stop)
                    newrow = {'time': date, 'sell_quantity': itr['sell'][i]['quantity'], 'sell_price': itr['sell'][i]['price'],
                              'sell_orders': itr['sell'][i]['orders'], 'buy_quantity': itr['buy'][0]['quantity'],
                              'buy_price': itr['buy'][0]['price'], 'buy_orders': itr['buy'][0]['orders']}
                    if newrow['sell_quantity']>minask:
                        # saveohlcshort(date,token,newrow['buy_price'])
                        trigger(token,date, newrow['buy_price'], 'short', short_target,short_stop)
                except:
                    pass
    print(positions)
    print(trades)
def append_results(newrow):
    frame=pd.read_csv('backtest.csv')
    frame=pd.DataFrame(frame)
    frame=frame.append(newrow, ignore_index=True)
    frame.to_csv('backtest.csv',index=False)

tokens=[16568066,16499970,16561922,16617218]
ltpdf(tokens)
minbid={16568066:60000,16499970:20000,16561922:25000,16617218:70000}
minask={16568066:60000,16499970:20000,16561922:25000,16617218:70000}
long_target={16568066:[1,2,3,1,2,3],16499970:[10,15,20,10,15,30],16561922:[20,30,40,10,15,30],16617218:[1,2,3,1,2,3]}
long_stop={16568066:[2,1,1.5,4.5,7,3],16499970:[10,10,10,6,20,15],16561922:[20,20,20,6,20,15],16617218:[2,1,1.5,4.5,7,3]}
short_target={16568066:[1,2,3,1,2,3],16499970:[10,15,20,10,15,30],16561922:[20,30,40,10,15,30],16617218:[1,2,3,1,2,3]}
short_stop={16568066:[2,1,1.5,4.5,7,3],16499970:[10,10,10,6,20,15],16561922:[20,20,20,6,20,15],16617218:[2,1,1.5,4.5,7,3]}
tc={16568066:158,16499970:172,16561922:192,16617218:328}
lotsize={16568066:3200,16499970:200,16561922:300,16617218:5700}

for token in tokens:
    for i in range(0,6):
        lontar=long_target[token][i]
        lonstop=long_stop[token][i]
        shortar =short_target[token][i]
        shorstop=short_stop[token][i]
        backtest(token,minbid[token],minask[token],lontar,lonstop,shortar,shorstop)
        newrow={'Script':ibt(token),'Long Target':lontar,'Long Stop':lonstop,'Short Target':shortar,
                'Short Stop':shorstop,'Min Bids':minbid,'Min Asks':minask,'Long Losses':trades['long_loss'],
                'Long Wins':trades['long_win'],'Short Wins':trades['short_win'],'Short Losses':trades['short_loss'],
                'Net Long Win':positions['long'],'Net Short Win':positions['short'],
                'Long Gross':((trades['long_win']*lontar)-(trades['long_loss']*lonstop))*lotsize[token],'Long TC':(trades['long_win']+trades['long_loss'])*tc[token],
                'Long Net':((trades['long_win']*lontar)-(trades['long_loss']*lonstop))*lotsize[token]-(trades['long_win']+trades['long_loss'])*tc[token],
                'Short Gross': ((trades['short_win'] * shortar) - (trades['short_loss'] * shorstop)) * lotsize[token],
                'Short TC': (trades['short_win'] + trades['short_loss']) * tc[token],
                'Short Net': ((trades['short_win'] * shortar) - (trades['short_loss'] * shorstop)) * lotsize[token] - (trades['short_win'] + trades['short_loss']) * tc[token]}
        append_results(newrow)
