def show_tradelist(ledger,open_positions,tradelist,timestamp,option_ltps,xl_put_spreads,xl_call_spreads):
    expiry = find_expiry()
    tradelist = dict(sorted(tradelist.items(), key=lambda item: item[1]))
    for key in tradelist.keys():
        if tradelist[key] > 0:
            price = 1.01 * option_ltps[key]
        else:
            price = 0.99 * option_ltps[key]
        nr = {'timestamp': timestamp, 'expiry': expiry, 'instrument': key,
              'qty': tradelist[key] * 75, 'price': price}
        nr['oid'] = send_option_market_order()
        ledger = ledger.append(nr, ignore_index=True)
        if key in open_positions.keys():
            open_positions[key] += tradelist[key]*75
        else:
            open_positions[key] = tradelist[key]*75
    poplist=[]
    for key in open_positions.keys():
        if open_positions[key] == 0:
            poplist.append(key)
    for key in poplist:
        open_positions.pop(key)
    print_ledger = ledger.copy()
    print_ledger['value'] = -ledger['qty'] * ledger['price']
    sqpnl = 0
    for key in open_positions.keys():
        sqpnl += option_ltps[key]*open_positions[key]

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
    pnldict = {'Open P/L':openpnl, 'Booked P/L':sum(booked_pnl.values()), 'Net P/L': print_ledger['value'].sum() + sqpnl}

    book.app.screen_updating = False
    s.range('F1').value = open_positions
    s.range('R1').value = print_ledger
    s.range('F6').value = pnldict
    s.range('F16').value = booked_pnl
    s.range('A1').value = xl_put_spreads
    s.range('C1').value = xl_call_spreads
    book.app.screen_updating = True
    return ledger,open_positions

def trade_spread(tradelist):
    tradelist = dict(sorted(tradelist.items(), key=lambda item: item[1]))
    oids = {}
    for key in tradelist.keys():
        qty = curr_ins[]*abs(tradelist[key])
        if tradelist[key]>0:
            typ = 'buy'
        else:
            typ = 'sell'
        token = engts[key]
        oids[key] = send_option_market_order(typ,token,qty)
    status = {}
    time.sleep(1)
    for oid in oids.keys():
        status[oid] = zd.kite.order_history(oid)[-1]['status']
    filled, close = 0,0
    for keys in status:
        if status[oid] == 'COMPLETE':
            filled = oid
        else:
            close = 1
    if close:
        for key in tradelist.keys():
            if oids[key] == filled:
                qty = 75 * abs(tradelist[key])
                if tradelist[key] > 0:
                    typ = 'sell'
                else:
                    typ = 'buy'
                token = engts[key]
                send_option_market_order(typ, token, qty)
        return 0
    else:
        return 1




def send_option_market_order(type, token, qty):
    if type == 'buy':
        tt = zd.kite.TRANSACTION_TYPE_BUY
    else:
        tt = zd.kite.TRANSACTION_TYPE_SELL
    try:
        order_id = zd.kite.place_order(tradingsymbol=token,
                                       exchange=zd.kite.EXCHANGE_NFO,
                                       transaction_type=tt,
                                       quantity=qty,
                                       order_type=zd.kite.ORDER_TYPE_MARKET,
                                       product=zd.kite.PRODUCT_NRML,
                                       variety='regular')

        print("Order placed. ID is: {}".format(order_id))
        return order_id
    except Exception as e:
        print("Order placement failed: {}".format(e))
        return order_id
