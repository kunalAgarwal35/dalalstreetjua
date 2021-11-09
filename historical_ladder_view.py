import pickle
import os
import datetime
import numpy as np
import pandas as pd

ts={}

with open('March.instruments', 'rb') as handle:
    instruments = (pickle.load(handle))
handle.close()
def ibat(x):
    for ins in instruments:
        if ins['instrument_token'] == x:
            return ins['tradingsymbol']
def nifty250fut_tokens():
    csv=pd.read_csv("nifty250.csv")
    symbols=list(csv['Symbol'])
    tokens=[]
    instruments=[]
    types = ['FUT']
    expfut = 100
    with open('May.instruments', 'rb') as handle:
        instru = (pickle.load(handle))
    handle.close()
    now = datetime.datetime.now()
    for ins in instru:
        if ins['instrument_type'] == 'FUT' and ins['name'] in symbols:
            days = (ins['expiry'] - now.date()).days
            if days < expfut:
                # print(ins['tradingsymbol'])
                expfut = days
    for ins in instru:
        if ins['name'] in symbols and ins['instrument_type']=='FUT':
            daystoexpiry = (ins['expiry'] - now.date()).days
            # print(daystoexpiry)
            if daystoexpiry == expfut:
                # print(ins['tradingsymbol'])
                tokens.append(ins['instrument_token'])
                ts[ins['instrument_token']]=ins['tradingsymbol']
                instruments.append(ins)
    return tokens

n50toks=nifty250fut_tokens()

df=pd.DataFrame(columns=('Volume','OI','Asks','Sell_Orders','Sell_Qty','Price','Bids','Buy_Orders','Buy_Qty','Time'))
df.to_csv('LadderSnap.csv',index=False)
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in os.listdir('oldticks')]
dates=[]
for item in dates_list:
    if item.date() not in dates:
        dates.append(item.date())
previous_tick={}
def readtick(filename,token):

    csv=pd.read_csv('LadderSnap.csv')
    with open('oldticks/'+filename.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
        tick = (pickle.load(handle))
        tick=tick[token]
    handle.close()
    if 'previoustick.pickle' in os.listdir():
        with open('previoustick.pickle', 'rb') as handle:
            previous_tick = (pickle.load(handle))
        handle.close()
        vol_traded=tick['volume']-previous_tick['volume']
        oi_change=tick['oi']-previous_tick['oi']
        price=tick['last_price']
        if price not in csv['Price']:
            newrow={'Volume':vol_traded,'OI':oi_change,'Asks':'','Price':price,
                    'Bids':'','Buy_Qty':tick['buy_quantity'],'Sell_Qty':tick['sell_quantity'],
                    'Time':filename.strftime("%m%d%Y-%H%M%S")}
            csv=csv.append(newrow,ignore_index=True)
        else:
            #if row exists
        buyorders=tick['depth']['buy']
        sellorders = tick['depth']['sell']
        for order in buyorders:
            bid=order['orders']
            price=order['price']
            buyqty=order['quantity']
            if price not in csv['Price']:
                newrow = {'Volume': '', 'OI': '', 'Asks': '', 'Price': price,
                          'Bids': buyqty, 'Buy_Qty': tick['buy_quantity'], 'Sell_Qty': '',
                          'Time': filename.strftime("%m%d%Y-%H%M%S")}
                csv=csv.append(newrow, ignore_index=1)
            else:
                #write if row exists

        for order in sellorders:
            ask = order['orders']
            price = order['price']
            sellqty = order['quantity']
            if price not in csv['Price']:
                newrow = {'Volume': '', 'OI': '', 'Asks': sellqty, 'Price': price,
                          'Bids': '', 'Sell_Qty': tick['sell_quantity'], 'Buy_Qty': '',
                          'Time': filename.strftime("%m%d%Y-%H%M%S")}
                csv = csv.append(newrow, ignore_index=1)
            else:
                #write if row exists







    previous_tick=tick
    pickle.dump(previous_tick, open('previoustick.pickle', "wb"))

