import pymongo
import os
import datetime
import pandas as pd


def test_for_empty_ts():
    empty_datasets = []
    for year in [2019, 2020, 2021]:
        for month in range(1, 13):
            if year == 2021:
                if month > 11:
                    break
            op_name = "{}_options".format(datetime.datetime(year, month, 1).strftime("%b_%Y").upper())
            conn = pymongo.MongoClient()
            cursor = conn['tick_data_csv'][op_name]
            data = cursor.find({"ts": ""}, {"_id": 0}).limit(10)

            l = list(data)
            if len(l) > 10:
                print("{} has {} empty ts".format(op_name, len(l)))
                empty_datasets.append(op_name)
    with open("empty_ts.txt", "w") as f:
        for item in empty_datasets:
            f.write("{}\n".format(item))



if __name__ == "__main__":
    test_for_empty_ts()