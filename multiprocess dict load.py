import pandas as pd
import pickle, os
import numpy as np
import datetime
from multiprocessing import Process, Manager
import time

ticklist = os.listdir('oldticks')[:15000]
dates_list = [datetime.datetime.strptime(date, "%m%d%Y-%H%M%S") for date in ticklist]

def updatedict(argu):
    d=argu[0]
    listofdates=argu[1]
    for item in listofdates:
        try:
            with open('oldticks/' + item.strftime("%m%d%Y-%H%M%S"), 'rb') as handle:
                tick = pickle.load(handle)
            handle.close()
            d[item]=tick
        except:
            continue
if __name__ == '__main__':
    t1=time.time()
    manager=Manager()
    tickdict = manager.dict()
    batchsize=300
    numprocess=len(dates_list)/batchsize
    numprocess=int(numprocess)
    p={}
    for i in range(0,numprocess+1):
        till=(i+1)*batchsize
        if till>len(dates_list):
            till=len(dates_list)
        print("From ",str(i*batchsize), " to ",str(till))
        p[i]=Process(target=updatedict,args=([tickdict,dates_list[batchsize*i:till]],))
        p[i].start()
    for i in range(0, numprocess + 1):
        p[i].join()
    print('Time Taken: ',time.time()-t1)
    print(len(tickdict))

