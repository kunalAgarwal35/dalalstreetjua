import xlwings as xw
import numpy as np
import pandas as pd

file_path = '12to2.xlsx'
book = xw.Book(file_path)
sheet = book.sheets['Sheet10']
df = sheet.range('A1:G100').value
returns = []
for row in df:
    for item in row:
        try:
            if 'Rs.' in item:
                item = item.replace('Rs.', '')
                item = item[:item.index(',')]
                item = item.replace(' ','')
                print(item)
                returns.append(float(item))
        except:
            pass

eq_curve = pd.DataFrame()
eq_curve['returns'] = returns
eq_curve['cum_returns'] = eq_curve['returns'].cumsum()
sheet.range('K1').value = eq_curve