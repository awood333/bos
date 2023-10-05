'''
status.py
'''
import pandas as pd
import numpy as np

f  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
f.set_index('datex', inplace=True)
f.index = pd.to_datetime(f.index)
bd  = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',index_col=0,header=0,parse_dates=['birth_date','death_date','arrived', 'adj_bdate'],low_memory=False,dtype=None)

# def adj_bdate(bd):
#     bd['adj_bdate'] = bd.apply(
#         lambda row: row['birth_date'] 
#         if pd.isna(row['arrived'])
#         else(row['arrived']) 
#         , axis=1
#         )
#     return bd

date_names = ['age cow','stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything = pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',index_col=0,header=0,parse_dates=date_names, date_format='%m/%d/%Y',low_memory=False,dtype=None)

# NOTE: 'status' reads from 'all' so don't make circul ar references

bdmax       = bd.index.max()                    # last cow WY_id
idx3        = range(1, bdmax + 1)
lastrow     = f.iloc[-1:,0].to_string(index=False)  # last row of milking values - floats

maxdate     = f.index.max()                  #timestamp of datex -  the index
mindate     = f.index.min()
startdate   = '2023-7-24'
stopdate    = '2023-9-24'

daterange   = pd.date_range(startdate, stopdate, freq='D') 

def alive(mask):
    mask =  np.zeros((len(daterange), bdmax), dtype=bool)
    for i, date_i in enumerate(daterange):
         
        for j in range(1, bdmax):  
             
            var1 = bd['adj_bdate'][j]
            var2 = bd['death_date'][j]
            
            condition = (
                (var1 <= date_i) &
                ((var2 > date_i) | (pd.isnull(var2) == True))
            )
            mask[i, j-1] = condition
  

    alive_mask = pd.DataFrame(mask, columns=idx3, index=daterange)
    return alive_mask

alive_mask = pd.DataFrame()     #next line wants a blank df
alive_mask = alive(alive_mask)

   
def  alive_count(alive_mask):
    # counts = []
    # for col in  alive_mask.columns:
    #     col_sum = np.sum(alive_mask[col] == True)
    #     counts.append(col_sum)
    # alive_count = pd.Series(counts, index=alive_mask.columns)
    return alive_mask.sum()

alive_count_df    = pd.DataFrame(alive_count(alive_mask.T))

def alive_ids(booldf, bval):
    alive_ids = booldf.apply(lambda col: np.where(col, col.index, ''), axis=0)
    return alive_ids
  
alive_ids_df       = pd.DataFrame(alive_ids(alive_mask.T, True))


f1 = f.loc[daterange].copy()

def milkers():
    mask = np.zeros((len(daterange), bdmax), dtype=bool)
    mask1, mask2, milkers_mask = [], [], []

    for i, date_i in enumerate(daterange, 0):
        for j in range(1, bdmax):
            mask1 = f1.iloc[i,j] > 0 
            mask2.append(mask1)
        milkers_mask.append(mask2)
        mask2 = []
    return milkers_mask
    

def milkers_ids(milkers_mask):
    milkers_ids = np.where(milkers_mask, np.arange(1,bdmax), np.nan)
    return milkers_ids

milkers_mask = milkers()
milkers_ids_df = pd.DataFrame(milkers_ids(milkers_mask))


alive_count_df .to_csv('F:\\COWS\\data\\status\\alive_count.csv')
alive_ids_df   .to_csv('F:\\COWS\\data\\status\\alive_ids.csv')
milkers_ids_df .to_csv('F:\\COWS\\data\\status\\milkers_ids.csv')

# #  write to csv
# sl3.to_csv('F:\\COWS\\data\\status\\status_lists.csv')
# df.to_csv('F:\\COWS\\data\\status\\status_column.csv')
# boxscore.to_csv('F:\\COWS\\data\\status\\boxscore.csv')


