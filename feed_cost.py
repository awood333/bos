'''
feed_cost.py
'''
import pandas as pd
import numpy as np
import datetime
import status 

corn1   = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\corn.csv', header=0, index_col='datex', parse_dates=['datex'])
# groupa  = pd.read_csv('F:\\COWS\\data\\status\\group_a_count_df.csv',      header=0, index_col='datex', parse_dates=['datex'])
# groupb  = pd.read_csv('F:\\COWS\\data\\status\\group_a_count_df.csv',      header=0, index_col='datex', parse_dates=['datex'])
# dry     = pd.read_csv('F:\\COWS\\data\\status\\dry_count.csv',          header=0, index_col='datex', parse_dates=['datex'])
# alive   = pd.read_csv('F:\\COWS\\data\\status\\alive_count.csv',      header=0, index_col='datex', parse_dates=['datex'])

# allcows = pd.concat([groupa, groupb, dry, alive], axis=1)


startdate   = '4/1/2023'
stopdate    = '10/7/2023'
date_range = pd.date_range(startdate, stopdate)

corn = corn1.reindex(date_range, method='ffill')
dfx = corn.merge(groupa, left_index=True, right_index=True )

cow_days = dfx['group_a'].T.sum()
dfx['cowdays'] = cow_days

daily_corn = (dfx['weight'] / dfx['cowdays']) * dfx['group_a']
dfx['daily_corn'] = daily_corn

print(dfx)








     

