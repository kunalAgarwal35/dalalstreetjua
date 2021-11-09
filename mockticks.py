# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 21:57:58 2021

@author: Kunal Agarwal
"""
import pandas as pd
import time
original=pd.read_csv('original.csv')
name='12428546_2021-03-03.csv'
def writetocsv(in_csv,out_csv):
    #get the number of lines of the csv file to be read
    number_lines = sum(1 for row in (open(in_csv)))
    
    #size of chunks of data to write to the csv
    chunksize = 1
    
    #start looping through data writing it to a new file for each chunk
    for i in range(0,number_lines,chunksize):
         df = pd.read_csv(in_csv,
              header=None,
              nrows = chunksize,#number of rows to read at each loop
              skiprows = i)#skip rows that have been read
    
         df.to_csv(out_csv,
              index=False,
              header=False,
              mode='a',#append data to csv file
              chunksize=chunksize)#size of data to append for each loop
         time.sleep(0.01)
    
writetocsv('original.csv',name)
