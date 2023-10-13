'''
status.py
'''
import pandas as pd
import numpy as np

f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
f.set_index('datex', inplace=True)
f.index = pd.to_datetime(f.index)

maxdate     = f.index.max()                  #timestamp of datex -  the index
startdate   = '2023-4-1'
stopdate    = maxdate

def create_date_range(startdate, stopdate):
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()

    return date_range, date_range_cols

date_range, date_range_cols = create_date_range(startdate, stopdate)



def create_alive_mask():   
    date_range = pd.date_range(startdate, stopdate, freq='D')
    
    bd  = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',
    index_col=0,
    header=0,
    parse_dates=['birth_date','death_date','arrived', 'adj_bdate'])
    
    bdmax = len(bd)
    idx3  = range(1, bdmax + 1)
    
    mask =  np.zeros([len(date_range), bdmax], dtype=bool)
    print('mask shape ',mask.shape)
    print('len daterange',len(date_range))
    
    for i, date_i in enumerate(date_range):
             
        var1 = bd['adj_bdate'][i+1]
        var2 = bd['death_date'][i+1] 

             
        condition = (
            (var1 <= date_i) &
            ((var2 > date_i) | (pd.isnull(var2) == True))
        )
        mask[:,i] = condition
        i+=1
        
    alive_mask = pd.DataFrame(mask, columns=idx3, index=date_range)
    alive_count_df = alive_mask.sum(axis=1).to_frame(name='alive')    
    return alive_mask, alive_count_df

alive_mask, alive_count_df = create_alive_mask()



def create_alive_ids(alive_mask):      #get col headers (WY_ids) for each live cow
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
    
    alive_ids_df = alive_mask.copy()         #creates a dataframe                 
    for col in alive_mask.columns: 
        alive_ids_df[col] = np.where(alive_mask[col], col, np.nan)
    return alive_ids_df



def create_milkers_mask():
    
    f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
    f.set_index('datex', inplace=True)
    f.index = pd.to_datetime(f.index)
    
    date_range = pd.date_range(startdate, stopdate, freq='D')
    
    f1 = f.loc[date_range].copy()    #partition the milk dbase
    milkers_mask        = f1>0   

    milk_mask_15down    = f1<15
    milkers_mask_df     = pd.DataFrame(milkers_mask, columns=f.columns, index=date_range)
    milkers_sum         = milkers_mask.sum(axis=1).to_frame()              #count of True in each row (date)
    milkers_count_df = pd.DataFrame(milkers_sum, index=date_range)
    return milkers_mask, milkers_mask_df, milkers_sum, milkers_count_df



def create_milkers_ids(milkers_mask):      # WY_id headers
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
    result = milkers_mask.copy()
    for col in milkers_mask.columns:
        result[col] = np.where(milkers_mask[col], col, np.nan)      #returns the col header where True, or np.nan for the False
    milkers_ids_df = pd.DataFrame(result, columns=milkers_mask.columns, index=date_range_cols)
    return milkers_ids_df



def create_group_a(days_milking_df):
    date_range = pd.date_range(startdate, stopdate, freq='D')
    days_df        = days_milking_df.copy()
    group_a        = days_df < 140          #create mask
    # group_a_df      = pd.DataFrame(milkers_mask, columns=f.columns, index=date_range)
    group_a_count        = group_a.sum(axis=1).to_frame(name='group_a')              #count of True in each row (date)
    group_a_count.index = date_range
    group_a_count.index.name = 'datex'
    # group_a_count_df    = pd.DataFrame(group_a_sum, index=date_range)    #, columns=['group a'])
    return group_a, group_a_count


    
def create_group_b(days_milking_df):
    date_range = pd.date_range(startdate, stopdate, freq='D')
    days_df        = days_milking_df.copy()
    group_b        = days_df >= 140          #create mask (df)
    group_b_count               = group_b.sum(axis=1).to_frame(name='group_b')              #count of True in each row (date)
    group_b_count.index         = date_range
    group_b_count.index.name    = 'datex'
    return group_b, group_b_count



def create_group_a_ids(group_a):      # WY_id headers
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
    
    result              = group_a.copy()
    for col in group_a.columns:
        result[col]     = np.where(group_a[col], col, np.nan)      #returns the col header where True, or np.nan for the False
    result.index        = date_range
    group_a_ids_df      = result #pd.DataFrame(result, columns=group_a.columns, index=date_range_cols)
    return group_a_ids_df



def create_group_b_ids(group_b):      # WY_id headers
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
    
    result              = group_b.copy()
    for col in group_b.columns:
        result[col]     = np.where(group_b[col], col, np.nan)      #returns the col header where True, or np.nan for the False
    groupb_ids_df       = result
    result.index        = date_range
    return groupb_ids_df



def create_days(milkers_mask):  
    
    f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
    f.set_index('datex', inplace=True)
    f.index = pd.to_datetime(f.index)
    
    lb_last = pd.read_csv('F:\\COWS\\data\\insem_data\\lb_last.csv',
    parse_dates=['lastcalf bdate'],
    index_col=0, header=0)  
    
    truecols = []
    days     = []
    fullday_data = {}
    f1       = f.loc[date_range].copy()    #partition the milk dbase
    
    for index, row in milkers_mask.iterrows():
        true_columns   = [col for col, value in row.items() if value == True]
        truecols.extend(true_columns)   #this accumulates the rows - o/wise only the last row is kept

    for index, row in milkers_mask.iterrows():
        days_result     = {}
        for col in truecols:
            if row[col] == True:        
                lastcalf_bdate  = lb_last.loc[int(col), 'lastcalf bdate'  ]
                date_diff       = (index - lastcalf_bdate).days
                days_result[col]    = date_diff

        days.append(days_result)
        days_milking_df = pd.DataFrame(days)
    return days_milking_df  



def create_dry_ids(alive_mask):
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
    
    f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
    f.set_index('datex', inplace=True)
    f.index = pd.to_datetime(f.index)
    
    # arrays for dry 
    milkers_mask        = f>0  
    alive_masknp = alive_mask.to_numpy()
    milkers_masknp = milkers_mask.to_numpy()
    dry_mask = np.logical_and(alive_masknp, np.logical_not(milkers_mask))
    dry_ids_df, dry_ids, dry_count_df   = create_dry_ids(dry_mask, date_range_cols)
    
    result = dry_mask.copy()
    for col in dry_mask.columns:
        result[col] = np.where(dry_mask[col], col, np.nan)
    dry_ids = result
    dry_ids_df = pd.DataFrame(result, columns=dry_mask.columns, index=date_range_cols)
    dry_count = dry_mask.sum(axis=1).to_frame(name='dry')
    dry_count.index = date_range
    dry_count.index.name = 'datex'
    return dry_ids, dry_ids_df, dry_count
    dry_ids = create_dry_ids(dry_mask)







