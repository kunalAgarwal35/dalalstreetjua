import pyautogui as pg
import time
import pandas as pd
import zd
zd.autologin()
n250=pd.read_csv('nifty250.csv')['Symbol'].to_list()
time.sleep(2)
allinstruments=kite.instruments()
def nifty250fut_ts():
    csv=pd.read_csv("nifty250.csv")
    symbols=list(csv['Symbol'])
    tokens=[]
    instruments=[]
    types = ['FUT']
    expfut = 100
    instru=kite.instruments()
    now = datetime.datetime.now()
    for ins in instru:
        if ins['instrument_type'] == 'FUT' and ins['name'] in symbols:
            days = (ins['expiry'] - now.date()).days
            if days < expfut:
                print(ins['tradingsymbol'])
                expfut = days
    for ins in instru:
        if ins['name'] in symbols and ins['instrument_type']=='FUT':
            daystoexpiry = (ins['expiry'] - now.date()).days
            print(daystoexpiry)
            if daystoexpiry == expfut:
                print(ins['tradingsymbol'])
                tokens.append(ins['tradingsymbol'])
                instruments.append(ins)
    return tokens

for item in nifty250fut_ts():
    pg.typewrite(item)
    pg.typewrite('\n')
    time.sleep(0.2)
    pg.click(pg.position())
    time.sleep(0.1)
