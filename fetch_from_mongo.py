import pymongo
import os
import pickle
import datetime
import pandas as pd


class MongoFetch(object):

    def __init__(self):
        self.mongoconn = pymongo.MongoClient()


    def get_data_for_expiry(self: object, sym: str, save: bool=True) -> dict:
        unqiue_expiries_dict = self.mongoconn["expiry_db"]["expiry_db"].find()
        unique_expiries = set()
        for expiry in unqiue_expiries_dict:
            for exp in expiry["expiry_dates"]:
                unique_expiries.add(exp)
        for expiry in unique_expiries:
            if expiry.month == 1 and expiry.year == 2021:
                continue
            if expiry.month == 12 and expiry.year  == 2020:
                continue
            if os.path.isfile("mongodb_cached_files_anim/ltps_{}_{}.pickle".format(sym, expiry.strftime("%d%b%Y"))):
                print("Found values for {} in cache".format(expiry.strftime("%d%b%Y")))
                continue
            t0 = expiry - datetime.timedelta(days=60)
            months = pd.date_range(
                t0,
                expiry,
                freq='MS').strftime("%b_%Y").tolist()
            # print(months)
            print(expiry, t0, months)
            exp_data = dict()
            for month in months:
                coll = "{}_options".format(month.upper())
                collection = self.mongoconn["tick_data_csv"][coll]
                # Expiration,Days,Strike,Call Bid,Call Ask,Put Bid,Put Ask
                data = collection.find(
                    {
                        "sym": sym,
                        "exp_d": expiry
                    },
                    {
                        "_id": 0,
                        "ts": 1,
                        "bp": 1,
                        "sp": 1,
                        "oi": 1,
                        "str": 1,
                        "op_typ": 1
                    }
                )
                data = list(data)
                print("Fetched {} data for {}".format(coll, sym))
                print("Data length: {}".format(len(data)))
                unique_timestamps = set()
                for row in data:
                    unique_timestamps.add(row["ts"])
                dumping_data = {ts: {"ltps": {}, "ois": {}, "bidasks": {}} for ts in list(unique_timestamps)}
                for row in data:
                    dumping_data[row["ts"]]["ltps"]["{}{}".format(int(row["str"]), row["op_typ"])] = (float(row["bp"])+ float(row["sp"]))/2
                    dumping_data[row["ts"]]["ois"]["{}{}".format(int(row["str"]), row["op_typ"])] = float(row["oi"])
                    dumping_data[row["ts"]]["bidasks"]["{}{}".format(int(row["str"]), row["op_typ"])] = float(row["sp"]) - float(row["bp"])
                exp_data.update(dumping_data)
            print("Processed {}".format(coll))
            if save:
                self.save_data(dumping_data, expiry, sym)
            print("Saved {}".format(coll))

    def save_data(self, data, expiry, sym):
        # if not os.path.isdir("mongodb_cached_files_anim"):
        #     os.mkdir("mongodb_cached_files_anim")
        with open("mongodb_cached_files_anim/ltps_{}_{}.pickle".format(sym, expiry.strftime("%d%b%Y"), ), "wb") as f:
            pickle.dump(data, f)

if __name__ == "__main__":
    mf = MongoFetch()
    dfs = mf.get_data_for_expiry("NIFTY", True)
    # print(dfs)