import datetime
from datetime import date
import time
import pandas as pd
import os

expiry = date(2015,1,29)
dayformat="%d-%m-%Y"
final_dayformat = '%y-%m-%d'
parent_folder = 'nse_options_historical/'

def find_strike_list(symb,expiry):
    import nsepy as nse
    month1 = datetime.timedelta(days = 60)
    step = 5
    stopped = 0
    started = 0
    strike = step
    ohlc = {}
    blanks = []
    _ = 0
    first_strike = 0
    second_strike = 0
    acc = 0
    stepfound = 0
    while not stopped and strike < 80000:
        print(strike)
        try:
            stock_opt = nse.get_history(symbol = symb,
                                    start=expiry - month1,
                                    end=expiry,
                                    option_type="CE",
                                    strike_price=strike,
                                    expiry_date=expiry)
        except:
            stock_opt = []
        if len(stock_opt):
            _ = 0
            ohlc[str(strike)+'CE'] = stock_opt
            ohlc[str(strike)+'PE'] = nse.get_history(symbol = symb,
                                    start=expiry - month1,
                                    end=expiry,
                                    option_type="PE",
                                    strike_price=strike,
                                    expiry_date=expiry)
            print(strike,' Saved')
            if first_strike and not second_strike:
                second_strike = strike
                step = second_strike - first_strike
                stepfound = 1
            if not started:
                first_strike = strike
                started = 1
                step = 5
                print('started')

            # print(strike)
        elif started:
            _ += 1
        if not started:
            acc += step
            # print('accumulated: ',acc)
            if acc == 100 and not stepfound and step<10:
                step = 10
            if acc == 500 and not stepfound and step<100:
                step = 100
            if acc == 5000 and not stepfound and step<500:
                step = 500
        if started and first_strike and second_strike and _ > 20:
            stopped = 1
            # print('stopped')
        strike = strike + step
    return ohlc

# symbols = pd.read_csv('nifty250.csv')['Symbol'].to_list()
# symbols.pop(0)
# symbols.pop(0)
symbols = ['RELIANCE','BRITANNIA','TATAMOTORS','MARUTI','CIPLA','BHARTIARTL','BAJFINANCE','EICHERMOT','GODREJCP','HDFC','BAJAJFINSV','DIVISLAB','HINDUNILVR','HINDALCO','ICICIBANK','TATAPOWER','TATASTEEL','WIPRO']

expiries = [datetime.datetime.strptime(i,dayformat).date() for i in open('monthly_expiries.txt','r').read().split('\n')]

#Uncomment for nifty 50 expiries
# temp = open('temp.txt','r').read()
# temp = temp.split('"')
# expiries = []
# for item in temp:
#     try:
#         # print(item)
#         exp = datetime.datetime.strptime(item,'%d-%m-%Y')
#         if exp not in expiries:
#             expiries.append(exp.date())
#     except:
#         pass


to_do = symbols
# to_do = ['BANKNIFTY']

for symb in to_do:
    print(symb)
    cont = 0
    for expiry in expiries:
        if cont or expiry > datetime.datetime.now().date():
            continue
        print(expiry)

        savedir = parent_folder + symb + '/'
        foldername = expiry.strftime(final_dayformat)
        ohlc_save_dir = savedir + foldername + '/'
        try:
            if len(os.listdir(ohlc_save_dir)):
                continue
        except:
            pass
        ohlc = find_strike_list(symb, expiry)
        if len(ohlc):
            if symb not in os.listdir(parent_folder):
                os.mkdir(parent_folder + symb)
            if foldername not in os.listdir(savedir):
                os.mkdir(savedir + foldername)
            for key in ohlc.keys():
                print(key)
                ohlc[key].to_csv(ohlc_save_dir+key+'.csv')
        # else:
        #     cont = 1

