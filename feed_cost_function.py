'''
'''
import pandas as pd
import numpy as np
import datetime
import status 



def create_feed_data_timeline(price, sequence,bunker_lot, weight, event):
    corn        = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\corn.csv',      header=0, index_col=None, parse_dates=['datex'])
    cassava     = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\cassava.csv',       header=0, index_col=None, parse_dates=['datex'])
    beans       = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\yellow_beans.csv',  header=0, index_col=None, parse_dates=['datex'])
    
    amt_cassava = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\cassava_amount.csv',   header=0, index_col=None, parse_dates=['datex'])
    amt_yellow_bean = pd.read_csv('F:\\COWS\\data\\csv_files\\feed_csv\\yellow_bean_amount.csv',   header=0, index_col=None, parse_dates=['datex'])

    feed = corn

    startdate   = '7/1/2023'
    stopdate    = '10/1/2023'

    date_range = status.create_date_range(startdate, stopdate)


    # lop = None  # Last Observed Price
    price       = pd.DataFrame()
    sequence    = pd.DataFrame()
    bunker_lot  = pd.DataFrame()
    weight      = pd.DataFrame()
    event       = pd.DataFrame()

    timeline    = pd.DataFrame(index=date_range)

    for index, row in feed.iterrows():
        if not pd.isna(row['price']):
            lop             = row['price']             #Last Observed Price
            last_sequence   = row['sequence']
            last_bunker     = row['bunker_lot']
            last_weight     = row['weight']

        if last_sequence is not None:
            start_date  = row['datex']
            stop_date   = corn.iloc[index + 1]['datex'] if index + 1 < len(corn) else stopdate
            date_range_chunk = pd.date_range(start=start_date, end=stop_date, freq='D')

            price = pd.concat([price, pd.Series([lop] * len(date_range_chunk), index=date_range_chunk)])
            sequence = pd.concat([sequence, pd.Series([last_sequence] * len(date_range_chunk), index=date_range_chunk)])
            bunker_lot = pd.concat([bunker_lot, pd.Series([last_bunker] * len(date_range_chunk), index=date_range_chunk)])
            weight = pd.concat([weight, pd.Series([last_weight] * len(date_range_chunk), index=date_range_chunk)])

    for index, row in corn.iterrows():
        if row['event']     == 'start':
            start_date      =   row['datex']
        elif row['event']   == 'stop':
            stop_date       =   row['datex']
            timeline.loc[start_date:stop_date, event] = row[event]

    timeline = pd.concat([sequence,bunker_lot, price, weight, event], axis=1)

    return timeline

feed_data_timeline = create_feed_data_timeline('sequence','bunker_lot', 'price', 'weight', 'event')
print(feed_data_timeline)