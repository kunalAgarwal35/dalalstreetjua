import pickle
import datetime
import time
import configparser
from multiprocessing import Process, Manager
def alert(timestamp, data, dfs_open_trades, oi_thresh, price_percent_thresh):
    t1=time.time()
    dfs = dfs_open_trades[0]
    open_trades = dfs_open_trades[1]
    cols = ['timestamp', 'buy_depth_price_0', 'sell_depth_price_0', 'last_price', 'oi', 'oic', 'pc', 'ratio', 'per_oic',
            'per_pc']
    start = time.time()
    txt = ''
    d = {}
    d['timestamp'] = timestamp
    d['buy_depth_price_0'] = data[key]['depth']['buy'][0]['price']
    d['sell_depth_price_0'] = data[key]['depth']['sell'][0]['price']
    d['last_price'] = data[key]['last_price']
    d['oi'] = data[key]['oi']

    if len(dfs[key]) == 0:
        d['oic'] = 0
        d['pc'] = 0
        d['ratio'] = 0
        d['per_oic'] = 0
        d['per_pc'] = 0
        dfs[key] = dfs[key].append(d, ignore_index=True)
        d = {}

    elif len(dfs[key]) == 1 and 100 * abs(np.log(data[key]['oi'] / dfs[key].iloc[0]['oi'])) > oi_thresh:
        d['oic'] = data[key]['oi'] - dfs[key].iloc[0]['oi']
        d['pc'] = data[key]['last_price'] - dfs[key].iloc[0]['last_price']
        d['ratio'] = 0
        d['per_oic'] = 100 * np.log(data[key]['oi'] / dfs[key].iloc[0]['oi'])
        d['per_pc'] = 100 * np.log(data[key]['last_price'] / dfs[key].iloc[0]['last_price'])
        dfs[key] = dfs[key].append(d, ignore_index=True)
        d = {}

    elif len(dfs[key]) == 2 and 100 * abs(np.log(data[key]['oi'] / dfs[key].iloc[1]['oi'])) > oi_thresh:
        d['oic'] = data[key]['oi'] - dfs[key].iloc[1]['oi']
        d['pc'] = data[key]['last_price'] - dfs[key].iloc[1]['last_price']
        pc = dfs[key].iloc[1]['last_price'] - dfs[key].iloc[0]['last_price']
        oic = dfs[key].iloc[1]['oi'] - dfs[key].iloc[0]['oi']
        d['ratio'] = (d['pc'] / d['oic']) / (pc / oic)
        d['per_oic'] = 100 * np.log(data[key]['oi'] / dfs[key].iloc[len(dfs[key]) - 1]['oi'])
        d['per_pc'] = 100 * np.log(data[key]['last_price'] / dfs[key].iloc[len(dfs[key]) - 1]['last_price'])

        dfs[key] = dfs[key].append(d, ignore_index=True)
        d = {}

        n = len(dfs[key])
        p = dfs[key].copy()

        if p.iloc[n - 1]['oic'] > 0 and p.iloc[n - 1]['ratio'] > 1 and abs(
                p.iloc[n - 1]['per_pc']) > price_percent_thresh:
            trade_data = {}
            trade_data['Entry time'] = timestamp
            trade_data['Token'] = key
            trade_data['per_oic'] = p.iloc[n - 1]['per_oic']
            trade_data['per_pc'] = p.iloc[n - 1]['per_pc']
            trade_data['ratio'] = p.iloc[n - 1]['ratio']

            if p.iloc[n - 1]['pc'] > 0:
                trade_data['Type'] = 'long'
                entry_price = p.iloc[n - 1]['sell_depth_price_0']
                trade_data['Entry price'] = entry_price
                print('Entering long at ', trade_data['Entry time'], 'Entry price : ', entry_price)
                # if trade:
                #     send_order_nfotoequity('buy',key)
                txt = txt + 'Alert for Entering LONG\n Entry price = ' + str(entry_price) + '\n' + ts[
                    key] + '\n' + url + '\n\n'
            if p.iloc[n - 1]['pc'] < 0:
                # Alert for entering short
                trade_data['Type'] = 'short'
                entry_price = p.iloc[n - 1]['buy_depth_price_0']
                trade_data['Entry Price'] = entry_price
                print('Entering short at ', trade_data['Entry time'], 'Entry price : ', entry_price)
                # if trade:
                #     send_order_nfotoequity('sell', key)
                txt = txt + 'Alert for Entering SHORT\n Entry price = ' + str(entry_price) + '\n' + ts[
                    key] + '\n' + url + '\n\n'
            open_trades[key] = trade_data

        elif p.iloc[n - 1]['oic'] > 0 and p.iloc[n - 1]['ratio'] < 1:
            if key in list(open_trades.keys()):
                # Alert for exitting
                if open_trades[key]['Type'] == 'long':
                    exit_price = p.iloc[n - 1]['buy_depth_price_0']
                    # if trade:
                    #     send_order_nfotoequity('sell', key)
                    txt = txt + 'Alert for Exiting LONG\n Exit price = ' + str(exit_price) + '\n' + ts[
                        key] + '\n' + url + '\n\n'
                else:
                    exit_price = p.iloc[n - 1]['sell_depth_price_0']
                    # if trade:
                    #     send_order_nfotoequity('buy', key)
                    txt = txt + 'Alert for Exiting SHORT\n Exit price = ' + str(exit_price) + '\n' + ts[
                        key] + '\n' + url + '\n\n'

                print('Exiting ', open_trades[key]['Type'], ' at ', p.iloc[n - 1]['timestamp'], 'Exit price : ',
                      exit_price)

                open_trades.pop(key)
