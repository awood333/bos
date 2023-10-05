'''
feed_cost.py
'''
import pandas as pd
import numpy as np
import datetime
from status import alive_count_df

bunkers1    = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bunkers1.csv',      header=0, index_col=None, parse_dates=['datex'])
bunkers2    = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bunkers2.csv',      header=0, index_col=None, parse_dates=['datex'])
tapioca     = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\tapioca.csv',       header=0, index_col=None, parse_dates=['datex'])
bag1        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bag1.csv',          header=0, index_col=None, parse_dates=['datex'])
bag2        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bag2.csv',          header=0, index_col=None, parse_dates=['datex'])
beans       = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\yellow_beans.csv',  header=0, index_col=None, parse_dates=['datex'])

amt_tapioca = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\amt_tapioca.csv',  header=0, index_col=None, parse_dates=['datex'])

b1  = pd.DataFrame(bunkers1)
b2  = pd.DataFrame(bunkers2)
bf  = pd.DataFrame(tapioca)


start_date = '4/1/2023'
end_date   = pd.to_datetime(datetime.date.today())
date_range  = pd.date_range(start=start_date, end=end_date, freq='D')

df_corn = pd.DataFrame(index=date_range)
# the value of the last expression in the interpreter to a particular variable called "_."

for _, row in bunkers2.iterrows():
    if not pd.isna(row['price']):
        lop = row['price']              # Last Observed Price
        last_sequence = row['sequence']
        last_bunker = row['bunker#']
        last_weight = row['weight']
    
    if last_sequence is not None: 
        start_date = row['datex']
        stop_date = bunkers2.iloc[_ + 1]['datex'] if _ + 1 < len(bunkers2) else end_date
        date_range_chunk = pd.date_range(start=start_date, end=stop_date, freq='D')
        
        price_series    = pd.concat([price_series,      pd.Series([lop] *               len(date_range_chunk), index=date_range_chunk)])
        sequence_series = pd.concat([sequence_series,   pd.Series([last_sequence] *     len(date_range_chunk), index=date_range_chunk)])
        bunker_series   = pd.concat([bunker_series,     pd.Series([last_bunker] *       len(date_range_chunk), index=date_range_chunk)])
        weight_series   = pd.concat([weight_series,     pd.Series([last_weight] *       len(date_range_chunk), index=date_range_chunk)])
     
for _, row in bunkers1.iterrows():
    if row['event'] == 'start':
        start_date = row['datex']
    elif row['event'] == 'stop':
        stop_date = row['datex']
        df_corn.loc[start_date:stop_date, 'bunker#'] = row['bunker#']
        
df_corn = pd.concat([sequence_series, bunker_series, price_series, weight_series,], axis=1)
        
print(df_corn[:3])
x=1