'''
'''
import pandas as pd
import numpy as np
import datetime

def process_feed_data(var1, var2, var3, var4, var5):
    bunkers1    = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bunkers1.csv',      header=0, index_col=None, parse_dates=['datex'])
    bunkers2    = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bunkers2.csv',      header=0, index_col=None, parse_dates=['datex'])
    tapioca     = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\tapioca.csv',       header=0, index_col=None, parse_dates=['datex'])
    bag1        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bag1.csv',          header=0, index_col=None, parse_dates=['datex'])
    bag2        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bag2.csv',          header=0, index_col=None, parse_dates=['datex'])
    beans       = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\yellow_beans.csv',  header=0, index_col=None, parse_dates=['datex'])
    amt_tapioca = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\amt_tapioca.csv',   header=0, index_col=None, parse_dates=['datex'])

    start_date  = '4/1/2023'
    end_date    = pd.to_datetime(datetime.date.today())
    date_range  = pd.date_range(start=start_date, end=end_date, freq='D')

    lop = None  # Last Observed Price
    var1_series = pd.DataFrame()
    var2_series = pd.DataFrame()
    var3_series = pd.DataFrame()
    var4_series = pd.DataFrame()
    var5_series = pd.DataFrame()

    df_corn = pd.DataFrame(index=date_range)

    for _, row in bunkers2.iterrows():
        if not pd.isna(row[var1]):
            lop             = row[var1]             #Last Observed Price
            last_sequence   = row[var2]
            last_bunker     = row[var3]
            last_weight     = row[var4]

        if last_sequence is not None:
            start_date  = row['datex']
            stop_date   = bunkers2.iloc[_ + 1]['datex'] if _ + 1 < len(bunkers2) else end_date
            date_range_chunk = pd.date_range(start=start_date, end=stop_date, freq='D')

            var1_series = pd.concat([var1_series, pd.Series([lop] * len(date_range_chunk), index=date_range_chunk)])
            var2_series = pd.concat([var2_series, pd.Series([last_sequence] * len(date_range_chunk), index=date_range_chunk)])
            var3_series = pd.concat([var3_series, pd.Series([last_bunker] * len(date_range_chunk), index=date_range_chunk)])
            var4_series = pd.concat([var4_series, pd.Series([last_weight] * len(date_range_chunk), index=date_range_chunk)])

    for _, row in bunkers1.iterrows():
        if row['event'] == 'start':
            start_date = row['datex']
        elif row['event'] == 'stop':
            stop_date = row['datex']
            df_corn.loc[start_date:stop_date, var5] = row[var5]

    df_corn = pd.concat([var2_series, var3_series, var1_series, var4_series], axis=1)

    return df_corn

result = process_feed_data('price','sequence','bunker#','weight','event')
print(result)
