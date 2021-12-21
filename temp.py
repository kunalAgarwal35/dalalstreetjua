import pickle
import os
import datetime
import traceback
import pymongo


# loc = "E:/ticks/"
#
# ticklist = os.listdir(loc)
# ticklist.sort()
# iupac = "%m%d%Y-%H%M%S-%f"
#
# last_tick = ticklist[-1]
# timestamplist = [datetime.datetime.strptime(i, iupac) for i in ticklist]
# tick = pickle.load(open(loc + last_tick, "rb"))
# key = list(tick.keys())[5]




"""
# keys to store
1. last_price
2. last_quantity
3. volume
4. buy_quantity -- rename to total_buy_quantity (tbq)
5. sell_quantity -- rename to total_sell_quantity (tsq)
6. last_trade_time
7. oi
8. 'depth': {'buy': [{'quantity': 75, 'price': 6668.45, 'orders': 1},
   {'quantity': 75, 'price': 6667.45, 'orders': 1},
   {'quantity': 75, 'price': 6665.45, 'orders': 1},
   {'quantity': 75, 'price': 6665.4, 'orders': 1},
   {'quantity': 75, 'price': 6664.7, 'orders': 1}],
  'sell': [{'quantity': 75, 'price': 6676.0, 'orders': 1},
   {'quantity': 75, 'price': 6676.2, 'orders': 1},
   {'quantity': 75, 'price': 6677.5, 'orders': 1},
   {'quantity': 75, 'price': 6677.55, 'orders': 1},
   {'quantity': 75, 'price': 6679.9, 'orders': 1}]},
9. name (this is sym in current mongo structure)
10. expiry
11. strike
12. instrument_type: {'FUT', 'CE', 'PE'}
"""

class KiteMongoDump(object):

    def __init__(self, dbname):
        self.iupac = "%m%d%Y-%H%M%S-%f"
        self.filename_format = self.iupac
        self.mongoconn = pymongo.MongoClient()
        self.db = self.mongoconn[dbname]
        self.futures_coll = self.db['{}_futures'.format(datetime.datetime.now().strftime("%b_%Y").upper())]
        self.options_coll = self.db['{}_options'.format(datetime.datetime.now().strftime("%b_%Y").upper())]

    @profile
    def main(self, filepath):
        ticklist = os.listdir(filepath)
        ticklist.sort()
        for i, tick in enumerate(ticklist):
            self.process_file(tick, filepath)
            if i % 100 == 0 or i == len(ticklist) - 1:
                print("Processed {}/{} - Latest file: {}".format(i+1, len(ticklist), tick))

    @profile
    def process_file(self, filename, filepath):
        abs_file_path = os.path.join(filepath, filename)
        timestamp = datetime.datetime.strptime(filename, self.filename_format)
        with open(abs_file_path, 'rb') as f:
            tick = pickle.load(f)
        fut_tick, opt_tick = self.process_tick(tick, timestamp)
        self.insert_tick(fut_tick, opt_tick)

    @profile
    def insert_tick(self, fut_tick, opt_tick):
        try:
            if fut_tick:
                self.futures_coll.insert_many(fut_tick)
            if opt_tick:
                self.options_coll.insert_many(opt_tick)
            return True
        except Exception:
            traceback.print_exc()
            return False

    @profile
    def process_tick(self, tick, timestamp):
        fut_tick, opt_tick = [], []
        for key in tick.keys():
            value = {}
            if tick[key]['instrument_type'] == 'EQ':
                continue
            elif tick[key]['instrument_type'] not in ["FUT", "CE", "PE"]:
                print("Unknown instrument type: {}".format(tick[key]['instrument_type']))
                continue
            value["ts"] = timestamp.replace(microsecond=0)
            if "last_price" in tick[key]:
                value["lp"] = float(tick[key]["last_price"])
            else:
                value["lp"] = 0.0
                print("Last price not found for {}".format(key))
            if "last_quantity" in tick[key]:
                value["lq"] = float(tick[key]["last_quantity"])
            else:
                value["lq"] = 0.0
                print("Last quantity not found for {}".format(key))
            if "volume" in tick[key]:
                value["vol"] = int(tick[key]["volume"])
            else:
                value["vol"] = 0
                print("Volume not found for {}".format(key))
            if "buy_quantity" in tick[key]:
                value["tbq"] = int(tick[key]["buy_quantity"])
            else:
                value["tbq"] = 0
                print("Buy quantity not found for {}".format(key))
            if "sell_quantity" in tick[key]:
                value["tsq"] = int(tick[key]["sell_quantity"])
            else:
                value["tsq"] = 0
                print("Sell quantity not found for {}".format(key))
            if "last_trade_time" in tick[key]:
                value["ltt"] = tick[key]["last_trade_time"]
            else:
                value["ltt"] = None
                print("Last trade time not found for {}".format(key))

            if "oi" in tick[key]:
                value["oi"] = int(tick[key]["oi"]) or 0
            else:
                value["oi"] = 0
                print("OI not found for {}".format(key))

            if "depth" in tick[key]:
                if len(tick[key]["depth"]["buy"]) > 0:
                    value["bq"] = int(tick[key]["depth"]["buy"][0]["quantity"])
                    value["bp"] = float(tick[key]["depth"]["buy"][0]["price"])

                if len(tick[key]["depth"]["sell"]) > 0:
                    value["sq"] = int(tick[key]["depth"]["sell"][0]["quantity"])
                    value["sp"] = float(tick[key]["depth"]["sell"][0]["price"])

                if len(tick[key]["depth"]["buy"]) > 1:
                    for i in range(1, len(tick[key]["depth"]["buy"])):
                        value["bq{}".format(i+1)] = int(tick[key]["depth"]["buy"][i]["quantity"])
                        value["bp{}".format(i+1)] = float(tick[key]["depth"]["buy"][i]["price"])

                if len(tick[key]["depth"]["sell"]) > 1:
                    for i in range(1, len(tick[key]["depth"]["sell"])):
                        value["sq{}".format(i+1)] = int(tick[key]["depth"]["sell"][i]["quantity"])
                        value["sp{}".format(i+1)] = float(tick[key]["depth"]["sell"][i]["price"])
            else:
                value["bq"] = 0
                value["bp"] = 0
                value["sq"] = 0
                value["sp"] = 0
                for i in range(1, 5):
                    value["bq{}".format(i)] = 0
                    value["bp{}".format(i)] = 0
                    value["sq{}".format(i)] = 0
                    value["sp{}".format(i)] = 0
                print("Depth not found for {}".format(key))

            if "name" in tick[key]:
                value["sym"] = tick[key]["name"]
            else:
                value["sym"] = key
                print("Name not found. Substituting with key: {}".format(key))

            if "exp" in tick[key]:
                value["exp"] = datetime.datetime.combine(tick[key]["expiry"], datetime.datetime.min.time())

            if "strike" in tick[key]:
                value["str"] = tick[key]["strike"]
            if tick[key]["instrument_type"] == "FUT":
                value["f_o"] = "f"
                fut_tick.append(value)
            elif tick[key]["instrument_type"] == "CE":
                value["f_o"] = "o"
                value["op_typ"] = "CE"
                opt_tick.append(value)
            elif tick[key]["instrument_type"] == "PE":
                value["f_o"] = "o"
                value["op_typ"] = "PE"
                opt_tick.append(value)

        return fut_tick, opt_tick

if __name__ == '__main__':
    filepath = "E:/ticks/"
    dbname = "kite_tick_db"
    kmd = KiteMongoDump(dbname)
    kmd.main(filepath)


