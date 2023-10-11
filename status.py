'''
status.py
'''
import pandas as pd
import numpy as np


lb_last = pd.read_csv('F:\\COWS\\data\\insem_data\\lb_last.csv',
    parse_dates=['lastcalf bdate'],
    index_col=0, header=0)

# days_df = pd.read_csv('F:\\COWS\\data\\status\\days_milking.csv', index_col=0)

f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
f.set_index('datex', inplace=True)
f.index = pd.to_datetime(f.index)
bd  = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',
    index_col=0,
    header=0,
    parse_dates=['birth_date','death_date','arrived', 'adj_bdate'],
    low_memory=False,dtype=None)

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
startdate   = '2023-9-1'
stopdate    = maxdate

def create_date_range(startdate, stopdate):
    date_range = pd.date_range(startdate, stopdate, freq='D')
    date_range_cols = date_range.strftime('%Y-%m-%d').to_list()
    len_date_range = len(date_range.tolist())
    return date_range, date_range_cols, len_date_range


def create_alive_mask(bd, date_range, len_date_range):   
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
    alive_count_df = alive_mask.sum(axis=1).to_frame()    
    return alive_mask, alive_count_df



def create_alive_ids(alive_mask, date_range_cols):      #get col headers (WY_ids) for each live cow
    alive_ids_df = alive_mask.copy()         #creates a dataframe                 
    for col in alive_mask.columns: 
        alive_ids_df[col] = np.where(alive_mask[col], col, np.nan)
    return alive_ids_df



def create_milkers_mask(f, date_range):
    f1 = f.loc[date_range].copy()    #partition the milk dbase
    milkers_mask        = f1>0   

    milk_mask_15down    = f1<15
    milkers_mask_df     = pd.DataFrame(milkers_mask, columns=f.columns, index=date_range)
    milkers_sum         = milkers_mask.sum(axis=1).to_frame()              #count of True in each row (date)
    milkers_count_df = pd.DataFrame(milkers_sum, index=date_range)
    return milkers_mask, milkers_mask_df, milkers_sum, milkers_count_df



def create_milkers_ids(milkers_mask, date_range_cols):      # WY_id headers
    result = milkers_mask.copy()
    for col in milkers_mask.columns:
        result[col] = np.where(milkers_mask[col], col, np.nan)      #returns the col header where True, or np.nan for the False
    milkers_ids_df = pd.DataFrame(result, columns=milkers_mask.columns, index=date_range_cols)
    return milkers_ids_df



def create_group_a(days_milking_df, date_range):
    days_df        = days_milking_df.copy()
    group_a        = days_df < 140          #create mask
    group_a_df      = pd.DataFrame(milkers_mask, columns=f.columns, index=date_range)
    group_a_sum         = group_a.sum(axis=1).to_frame()              #count of True in each row (date)
    group_a_count_df    = pd.DataFrame(group_a_sum, index=date_range)
    return group_a, group_a_df, group_a_sum, group_a_count_df


    
def create_group_b(days_milking_df, date_range):
    days_df        = days_milking_df.copy()
    group_b        = days_df >= 140          #create mask 
    group_b_df      = pd.DataFrame(milkers_mask, columns=f.columns, index=date_range)
    group_b_sum       = group_b.sum(axis=1).to_frame()              #count of True in each row (date)
    group_b_count_df  = pd.DataFrame(group_b_sum, index=date_range)   
    return group_b, group_b_df, group_b_sum, group_b_count_df



def create_group_a_ids(group_a, date_range_cols):      # WY_id headers
    result = group_a.copy()
    for col in group_a.columns:
        result[col] = np.where(group_a[col], col, np.nan)      #returns the col header where True, or np.nan for the False
    group_a_ids_df = pd.DataFrame(result, columns=group_a.columns, index=date_range_cols)
    return group_a_ids_df



def create_group_b_ids(group_b, date_range_cols):      # WY_id headers
    result = group_b.copy()
    for col in group_b.columns:
        result[col] = np.where(group_b[col], col, np.nan)      #returns the col header where True, or np.nan for the False
    groupb_ids_df = pd.DataFrame(result, columns=group_b.columns, index=date_range_cols)
    return groupb_ids_df



def create_days(f, milkers_mask, date_range, lb_last):    
    truecols = []
    days     = []
    fullday_data = {}
    f1       = f.loc[date_range].copy()    #partition the milk dbase
    
    for index, row in milkers_mask.iterrows():
        true_columns   = [col for col, value in row.items() if value == True]
        truecols.extend(true_columns)   #this accumulates the rows - o/wise only the last row is kept

    for index, row in milkers_mask.iterrows():
        days_result = {}
        for col in truecols:
            if row[col] == True:        
                lastcalf_bdate = lb_last.loc[int(col), 'lastcalf bdate'  ]
                date_diff   = (index - lastcalf_bdate).days
                days_result[col]    = date_diff

        days.append(days_result)
        days_milking_df = pd.DataFrame(days)
    return days_milking_df  


def create_dry_ids(dry_mask, date_range_cols):
    result = dry_mask.copy()
    for col in dry_mask.columns:
        result[col] = np.where(dry_mask[col], col, np.nan)
    dry_ids = result
    dry_ids_df = pd.DataFrame(result, columns=dry_mask.columns, index=date_range_cols)
    dry_count_df = dry_mask.sum(axis=1).to_frame()
    return dry_ids, dry_ids_df, dry_count_df



date_range, date_range_cols, len_date_range = create_date_range(startdate, stopdate)
milkers_mask, milkers_ids_df, milkers_sum, milkers_count_df = create_milkers_mask(f, date_range)
alive_mask, alive_count_df                  = create_alive_mask(bd, date_range, len_date_range)
alive_ids_df                                = create_alive_ids(alive_mask, date_range_cols) 

milkers_ids_df = create_milkers_ids(milkers_mask, date_range_cols)
days_milking_df        = create_days       (f, milkers_mask, date_range, lb_last)

group_a, group_a_df, group_a_sum, group_a_count_df           = create_group_a(days_milking_df, date_range)
group_a_ids         = create_group_a_ids(group_a, date_range_cols)
group_b, group_b_df, group_b_sum, group_b_count_df          = create_group_b(days_milking_df, date_range)
group_b_ids         = create_group_b_ids(group_b, date_range_cols)


# arrays for dry 
alive_masknp = alive_mask.to_numpy()
milkers_masknp = milkers_mask.to_numpy()
dry_mask = np.logical_and(alive_masknp, np.logical_not(milkers_mask))

dry_ids_df, dry_ids, dry_count_df   = create_dry_ids(dry_mask, date_range_cols)


# write to csv
alive_count_df  .to_csv('F:\\COWS\\data\\status\\alive_count.csv')
alive_ids_df    .to_csv('F:\\COWS\\data\\status\\alive_ids.csv')

milkers_ids_df  .to_csv('F:\\COWS\\data\\status\\milkers_ids.csv')
milkers_count_df.to_csv('F:\\COWS\\data\\status\\milkers_count.csv')

dry_ids_df      .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')
dry_count_df    .to_csv('F:\\COWS\\data\\status\\dry_count.csv')

days_milking_df .to_csv('F:\\COWS\\data\\status\\days_milking.csv')

group_a_df      .to_csv('F:\\COWS\\data\\status\\group_a_df.csv')
group_a_sum     .to_csv('F:\\COWS\\data\\status\\group_a_sum.csv')
group_a_count_df.to_csv('F:\\COWS\\data\\status\\group_a_count_df.csv')

group_b_df      .to_csv('F:\\COWS\\data\\status\\group_b_df.csv')
group_b_sum     .to_csv('F:\\COWS\\data\\status\\group_b_sum.csv')
group_b_count_df.to_csv('F:\\COWS\\data\\status\\group_b_count_df.csv')
