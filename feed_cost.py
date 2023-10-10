'''
feed_cost.py
'''
import pandas as pd
import numpy as np
import datetime
from status import create_date_range

f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
f.set_index('datex', inplace=True)
f.index = pd.to_datetime(f.index)


bunker1    = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bunker1.csv',      header=0, index_col=None, parse_dates=['datex'])
bunker2    = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bunker2.csv',      header=0, index_col=None, parse_dates=['datex'])
tapioca     = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\tapioca.csv',       header=0, index_col=None, parse_dates=['datex'])
# bag1        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bag1.csv',          header=0, index_col=None, parse_dates=['datex'])
# bag2        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\bag2.csv',          header=0, index_col=None, parse_dates=['datex'])
beans       = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\yellow_beans.csv',  header=0, index_col=None, parse_dates=['datex'])



amt_tapioca = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\amt_tapioca.csv',  header=0, index_col=None, parse_dates=['datex'])

b1  = pd.DataFrame(bunker1)
b2  = pd.DataFrame(bunker2)
bf  = pd.DataFrame(tapioca)


startdate  = '2023-4-15'
maxdate = f.index.max()
stopdate    = pd.to_datetime(datetime.date.today())
date_range  = pd.date_range(startdate, maxdate, freq='D')

colnames = bunker1.columns.tolist()

df_corn         = pd.DataFrame(columns=colnames)


# Iterate through the date range and fill in the values from bunker1
for date in date_range:
    row = bunker1[bunker1['datex'] <= date].iloc[-1]
    df_corn = pd.concat([df_corn, pd.DataFrame(row).transpose()], ignore_index=True)
# Reset the index and set the 'datex' column to the date range
df_corn.reset_index(drop=True, inplace=True)
df_corn['datex'] = date_range

# Fill NaN values with appropriate default values
# df_corn['sequence'].fillna(1, inplace=True)
# df_corn['event'] = df_corn.apply(lambda row: 'start' if row['datex'] == row['datex'].date() else '',axis=1)
# df_corn['bunker#'].fillna(2, inplace=True)



print(df_corn.iloc[:10,:])


#             sequence    typex      event  bunker#      datex
# 2023-04-01       1.0    start   duration      2.0 2023-04-15
# 2023-04-02       1.0    start   duration      2.0 2023-04-15
# 2023-04-03       1.0    start   duration      2.0 2023-04-15
# 2023-04-04       1.0    start   duration      2.0 2023-04-15
# 2023-04-05       1.0    start   duration      2.0 2023-04-15
# 2023-04-06       1.0    start   duration      2.0 2023-04-15
# 2023-04-07       1.0    start   duration      2.0 2023-04-15
# 2023-04-08       1.0    start   duration      2.0 2023-04-15
# 2023-04-09       1.0    start   duration      2.0 2023-04-15
# 2023-04-10       1.0    start   duration      2.0 2023-04-15
# ...
# 2023-07-22       1.0    start   duration      2.0 2023-04-15
# 2023-07-23       1.0     stop   duration      2.0 2023-07-23
# 2023-07-24       2.0    start   duration      1.0 2023-07-24
# 2023-07-25       2.0    start   duration      1.0 2023-07-24
# ...
# 2023-09-23       2.0    start   duration      1.0 2023-07-24
# 2023-09-24       2.0     stop   duration      1.0 2023-09-24
# ...
# 2023-10-06       2.0     stop   duration      1.0 2023-09-24













     

