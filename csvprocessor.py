# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 21:28:47 2021

@author: Kunal Agarwal
"""

import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

tod=str(datetime.datetime.today()).split()[0]
print("Looking for CSV")
fcsv=0

for file in os.listdir():
    if tod in file:
        df=pd.read_csv(file)
        print("Found CSV")
        fcsv=1
        break
if fcsv==0:
    file=input("Enter File name")
    df=pd.read_csv(file)
volum={}
prices={}
volum[0]=[]
prices[0]=[]
# df=pd.read_csv(file)
# while (len(df)-len(volume))>2:
#     volume.append(df['volume'][len(volume)+1]-df['volume'][len(volume)])
#     prices.append(df['ltp'][len(prices)+1])
#     print(df['timestamp'][len(prices)+1])
def animate(i):
    try:
        df=pd.read_csv(file, error_bad_lines=False)
    except:
        pass
    # print("Length of CSV: ",str(len(df)))
    try:
        for i in range(len(volum[0]),len(df)-1):
            vol=volum[0]
            vol.append(df['volume'][i+1]-df['volume'][i])
            volum[0]=vol
            pri=prices[0]
            pri.append(df['ltp'][i+1])
            prices[0]=pri
    except:
        pass
    
    # print(str(len(volum[0]))," :Volume ___ Prices:",str(len(prices[0])))
    try:
        print(df['timestamp'][len(prices[0])])
    except:
        pass
    plt.cla()
    plt.xlabel("Price") 
    plt.ylabel("Volume")
    plt.bar(prices[0],volum[0])
    # except:
    #     plt.cla()
    #     plt.xlabel("Price") 
    #     plt.ylabel("Volume")
    #     plt.bar(prices[0],volume[0])
        
    # df['dVol'] = df['volume'].shift(-1) - df['volume']
    # df=df.loc[df['dVol'] != 0]

ani=FuncAnimation(plt.gcf(),animate,interval=1000)
plt.tight_layout()
plt.show()
# while True:
#     try:
#         df=pd.read_csv(file)
#         while (len(df)-len(volume))>4:
#             volume.append(df['volume'][len(volume)+1]-df['volume'][len(volume)])
#             prices.append(df['ltp'][len(prices)+1])
# #             # plt.title(df['timestamp'][len(prices)+1])
#             print(df['timestamp'][len(prices)+1])
#         if len(df)>len(volume):
#             print("Showing")
#             volume.append(df['volume'][len(volume)+1]-df['volume'][len(volume)])
#             prices.append(df['ltp'][len(prices)+1])
#             # df['dVol'] = df['volume'].shift(-1) - df['volume']
#             # df=df.loc[df['dVol'] != 0]
        
#             # prices = list(df['ltp']) 
#             # values = list(df['dVol']) 
#             plt.cla()
#             fig = plt.figure(figsize = (10, 5)) 
          
#         # creating the bar plot 
#             plt.bar(prices, volume) 
          
#             plt.xlabel("Price") 
#             plt.ylabel("Volume")
#             plt.show() 
#     except:
#         pass

# for i in range(1,3000):
#     try:
#         df=pd.read_csv(file)
#         print(len(df))
#         time.sleep(0.2)
#     except:
#         print("Error No: ",str(i))