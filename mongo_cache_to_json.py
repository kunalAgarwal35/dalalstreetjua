import os
import pickle
import json

save_loc = 'C:\\Users\\Kunal\\OneDrive\\store\\mongo_json'
from_loc = 'mongo_cache'

for file in os.listdir(from_loc):
    savename = file.split('.')[0] + '_sample.json'
    if savename in os.listdir(save_loc):
        continue
    with open(os.path.join(from_loc, file), 'rb') as f:
        data = pickle.load(f)
    tslist = list(data.keys())
    jsonlist = []
    for ts in tslist[10000:10050]:
        ndt = data[ts]
        ndt['ts'] = str(ts)
        jsonlist.append(ndt)
    with open(os.path.join(save_loc, savename), 'w') as f:
        json.dump(jsonlist, f)
    print(savename)
    break


