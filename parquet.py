import pandas as pd


df = pd.read_parquet('numerai_training_data.parquet',engine='pyarrow')

for col in df.columns:
    if 'target' in col:
        i +
