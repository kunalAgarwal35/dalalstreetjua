import xlwings as xw
import pandas as pd
file_path='xlwings.xlsx'
df=pd.DataFrame(columns=["Time","Price"])
df=df.append({"Time":"5:40","Price":500},ignore_index=True)
xw.Book(file_path).sheets['Sheet1'].range('A1').value = df