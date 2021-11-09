import datetime
import zd
import pickle

instruments = zd.kite.instruments()

for ins in instruments:
    if ins['name'] == 'NIFTY 50' and ins['segment'] == 'INDICES':
        token = ins['instrument_token']
        fdate = datetime.date(2015,1,1)
        tdate = datetime.datetime.now().date()
        historical_data = zd.get_historical_force(token,fdate,tdate,'5minute',0)
        pickle.dump(historical_data,open('nifty_historical.pkl','wb'))
    if ins['name'] == 'INDIA VIX' and ins['segment'] == 'INDICES':
        token = ins['instrument_token']
        fdate = datetime.date(2015, 1, 1)
        tdate = datetime.datetime.now().date()
        historical_data = zd.get_historical_force(token, fdate, tdate, '5minute', 0)
        for column in historical_data.columns:
            if column in ['date','volume','oi']:
                continue
            hdc = []
            for item in historical_data[column].to_list():
                if item > 100:
                    if item > 1000:
                        hdc.append(item/100)
                    else:
                        hdc.append(item/10)
                else:
                    hdc.append(item)
            historical_data[column] = hdc
        pickle.dump(historical_data,open('vix_historical.pkl','wb'))
    if ins['name'] == 'NIFTY BANK' and ins['segment'] == 'INDICES':
        token = ins['instrument_token']
        fdate = datetime.date(2015,1,1)
        tdate = datetime.datetime.now().date()
        historical_data = zd.get_historical_force(token,fdate,tdate,'5minute',0)
        pickle.dump(historical_data,open('banknifty_historical.pkl','wb'))
