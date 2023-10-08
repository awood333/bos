import pandas as pd
import numpy as np
# from status import create_milkers_ids as cmi

lb_last = pd.read_csv('F:\\COWS\\data\\insem_data\\lb_last.csv',
    parse_dates=['lastcalf bdate'],
    index_col=0, header=0)

fullday1 = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',
    index_col='datex',
    header=0,
    parse_dates=['datex'])

startdate   = '2023-9-01'
stopdate    = '2023-9-30'

date_range = pd.date_range(startdate, stopdate, freq='D')
date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
len_date_range = len(date_range.tolist())

fullday = fullday1.loc[date_range,:]
print(fullday.iloc[:4,:6])




milkers_mask = fullday>0
lastcalf = lb_last[milkers_mask]




days= []
fullday_data = []
truecols = []

for index, row in milkers_mask.iterrows():
    true_columns   = [col for col, value in row.items() if value == True]
    truecols.extend(true_columns)   #this accumulates the rows - o/wise only the last row is kept

days     = []
fullday_data    = {}
    
for index, row in milkers_mask.iterrows():
    days_result = {}
    for col in truecols:
        if row[col] == True:        
            lastcalf_bdate = lb_last.loc[int(col), 'lastcalf bdate'  ]
            date_diff   = (index - lastcalf_bdate).days
            days_result[col]    = date_diff

    days.append(days_result)
    
    for col in truecols:
        fullday_data[col] = [fullday.loc[index, col] for index in date_range]

        

days_df     = pd.DataFrame(days,            index=milkers_mask.index)
fullday_df  = pd.DataFrame(fullday_data,    index=date_range)

x=1
days_df.to_csv('F:\\COWS\\data\\status\\days_milking.csv')
fullday_df.to_csv('F:\\COWS\\data\\status\\liters_milking.csv')
 
 