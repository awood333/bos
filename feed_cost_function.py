'''
feed_cost_function.py
'''
import pandas as pd
import numpy as np
import datetime


startdate   = '4/1/2023'
stopdate    = '10/1/2023'
date_range = pd.date_range(startdate, stopdate)


def create_feed_data_timeline():
    
    corn        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\corn.csv',
        header=0, index_col=0, parse_dates=['datex','open_date', 'end_date'])
    status1      =  pd.read_csv('F:\\COWS\\data\\status\\status_all.csv',
        header=0, index_col='datex', parse_dates=['datex'])

    status = status1.loc[date_range]        
 
    result = corn.reindex(date_range, method='ffill')
    result.index.name = 'datex'
    
    

    # duration column
    duration = (result['end_date'] - result['open_date']).dt.days
    result['duration'] = duration


    corn_df = pd.DataFrame(result)
    
    cc = corn_df.merge(status, left_index=True, right_index=True)
   
    cowdays3 = []
    avg_daily_amt1 = []

    for i in corn.sequence:

        bunkstart = corn['open_date'][i-1]
        bunkend     = corn['end_date'][i-1]
        bunk_range = pd.date_range(bunkstart, bunkend, freq='D')
        bunk_range2 = cc.loc[bunk_range].copy()   
        cowdays1 = bunk_range2['alive'].sum()
        cowdays2 = pd.Series(cowdays1, index=bunk_range)
        avg_daily_amt2 = (bunk_range2['weight'] / cowdays1)
     
        cowdays3.append(cowdays2)
        
        avg_daily_amt1.append(avg_daily_amt2)


    avg_daily_amt   = [item for sublist in avg_daily_amt1 for item in sublist]       #flattens the list from the loop
    cowdays        = [item for sublist in cowdays3       for item in sublist]       #flattens the list from the loop          
     
        
    groupa_consumption  = cc['group_a'] * avg_daily_amt
    groupb_consumption  = cc['group_b'] * avg_daily_amt
    dry_consumption     = cc['dry'] * avg_daily_amt    
    daily_consumption   = (groupa_consumption + groupb_consumption + dry_consumption)
    daily_corn_cost     = daily_consumption * cc['unit_price']
    
    
    cc['cowdays']       = cowdays
    cc['avg_daily_amt'] = avg_daily_amt
    cc['groupa consum'] = groupa_consumption
    cc['groupb consum'] = groupb_consumption
    cc['dry consum']    = dry_consumption
    cc['daily total']   = daily_consumption
    cc['daily corn cost'] = daily_corn_cost
    

    return corn_df, cc

corn_df, cc = create_feed_data_timeline()

cc.to_csv('F:\\COWS\\data\\feed_data\\feed_corn.csv')
x=1

