'''
status.py
'''
import pandas as pd
import numpy as np
import time




f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
f.set_index('datex', inplace=True)
f.index = pd.to_datetime(f.index)
bd  = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',
    index_col=0,
    header=0,
    parse_dates=['birth_date','death_date','arrived', 'adj_bdate'],
    low_memory=False,dtype=None)

# bd['adj_bdate'] = bd.apply(lambda row: row['birth_date'] if pd.isna(row['arrived']) else row['arrived'], axis=1)

date_names = ['age cow','stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything = pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',
    index_col=0,
    header=0,
    parse_dates=date_names, 
    date_format='%m/%d/%Y',
    low_memory=False,dtype=None)

bdmax       = bd.index.max()                    # last cow WY_id
idx3        = range(1, bdmax + 1)
lastrow     = f.iloc[-1:,0].to_string(index=False)  # last row of milking values - floats

maxdate     = f.index.max()                  #timestamp of datex -  the index
mindate     = f.index.min()
startdate   = '2023-7-24'
stopdate    = '2023-9-24'

date_range = pd.date_range(startdate, stopdate, freq='D')
date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
len_date_range = len(date_range.tolist())

def create_alive_mask(bd, date_range):
    
    mask =  np.zeros([len_date_range, bdmax], dtype=bool)
    for i, date_i in enumerate(date_range):
        for j in range(1, bdmax):  
             
            var1 = bd['adj_bdate'][j]
            var2 = bd['death_date'][j]
            
            condition = (
                (var1 <= date_i) &
                ((var2 > date_i) | (pd.isnull(var2) == True))
            )
            mask[i, j-1] = condition
            
    alive_mask = pd.DataFrame(mask, columns=idx3, index=date_range)
    return alive_mask

alive_mask = create_alive_mask(bd, date_range)
alive_count_df = alive_mask.sum().to_frame()

def create_alive_ids(alive_mask, date_range_cols):
    result = alive_mask.copy()  # Create a copy of alive_mask to store the results
    
    for col in alive_mask.columns:
        result[col] = np.where(alive_mask[col], col, np.nan)
    
    alive_ids_df = pd.DataFrame(result, columns=alive_mask.columns, index=date_range_cols)
    return alive_ids_df


alive_ids_df = create_alive_ids(alive_mask, date_range_cols)

f1 = f.loc[date_range].copy()

milkers_mask = f1>0

def create_milkers_ids(milkers_mask, date_range_cols):
    result = milkers_mask.copy()
    for col in milkers_mask.columns:
        result[col] = np.where(milkers_mask[col], col, np.nan)
    
    milkers_ids_df = pd.DataFrame(result, columns=milkers_mask.columns, index=date_range_cols)
    return milkers_ids_df

milkers_ids_df = create_milkers_ids(milkers_mask, date_range_cols)
milkers_count_df = milkers_mask.sum(axis=1).to_frame()


alive_masknp = alive_mask.to_numpy()
milkers_masknp = milkers_mask.to_numpy()
dry_mask = alive_masknp ^ milkers_masknp




def create_dry_ids(dry_mask, date_range_cols):
    result = dry_mask.copy()
    for col in dry_mask.columns:
        result[col] = np.where(dry_mask[col], col, np.nan)
    
    dry_ids_df = pd.DataFrame(result, columns=dry_mask.columns, index=date_range_cols)
    return dry_ids_df

dry_ids_df   = create_dry_ids(dry_mask, date_range_cols)
dry_count_df = dry_mask.sum(axis=1).to_frame() 

# write to csv
alive_count_df  .to_csv('F:\\COWS\\data\\status\\alive_count.csv', index=False)
alive_ids_df    .to_csv('F:\\COWS\\data\\status\\alive_ids.csv', index=False)
milkers_ids_df  .to_csv('F:\\COWS\\data\\status\\milkers_ids.csv')
milkers_count_df.to_csv('F:\\COWS\\data\\status\\milkers_count.csv')
dry_ids_df      .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')
dry_count_df    .to_csv(('F:\\COWS\\data\\status\\dry_count.csv'))

dry_count_df    .to_csv(('F:\\COWS\\data\\status\\alive1df.csv'))
milkers1df    .to_csv(('F:\\COWS\\data\\status\\milkers1df.csv'))

