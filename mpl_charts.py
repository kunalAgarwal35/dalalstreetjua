import mplfinance as mpf
import zd
import pickle
import pandas as pd
with open('April_kite.instruments', 'rb') as handle:
    old_ins_lst= (pickle.load(handle))
handle.close()
curr_ins_lst=zd.kite.instruments()
curr_ins={}
for ins in curr_ins_lst:
    curr_ins[ins['instrument_token']]=ins
old_ins={}
for ins in old_ins_lst:
    old_ins[ins['instrument_token']]=ins
def historical(token,fdate,tdate,interval,o_i):
    if type(token)==int and token in curr_ins.keys():
        ohlc=pd.DataFrame(zd.kite.historical_data(instrument_token=token,from_date=fdate,to_date=tdate,interval=interval,oi=o_i))
        return ohlc
    elif type(token)==str:
        for ins in curr_ins.keys():
            if curr_ins[ins]['tradingsymbol']==token:
                token=curr_ins[ins]['instrument_token']
                ohlc = pd.DataFrame(
                    zd.kite.historical_data(instrument_token=token, from_date=fdate, to_date=tdate, interval=interval,
                                            oi=o_i))
                return ohlc
        for ins in old_ins.keys():
            if old_ins[ins]['tradingsymbol']==token:
                token=old_ins[ins]['instrument_token']
                for new_key in curr_ins.keys():
                    if curr_ins[new_key]['tradingsymbol']==old_ins[key]['tradingsymbol']:
                        token=curr_ins[new_key]['instrument_token']
                        ohlc = pd.DataFrame(
                            zd.kite.historical_data(instrument_token=token, from_date=fdate, to_date=tdate, interval=interval,
                                                    oi=o_i))
                        return ohlc
    else:
        for key in old_ins.keys():
            if old_ins[key]['instrument_token']==token:
                for new_key in curr_ins.keys():
                    if curr_ins[new_key]['tradingsymbol']==old_ins[key]['name']:
                        token=curr_ins[new_key]['instrument_token']
                        ohlc = pd.DataFrame(
                            zd.kite.historical_data(instrument_token=token, from_date=fdate, to_date=tdate,
                                                    interval=interval,
                                                    oi=o_i))
                        return ohlc
def savechart(name,filepath,token,fdate,tdate,interval,o_i):
    plotdf=historical(token,fdate,tdate,interval,o_i)
    plotdf['date'] = plotdf['date'].dt.tz_localize(None)
    plotdf = plotdf.set_index('date')
    mpf.plot(plotdf, type='candlestick', title=name, show_nontrading=False, volume=True,
             savefig=filepath+'.png', style='charles')

