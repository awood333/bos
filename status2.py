import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
# 
import insem_ultra as iu
import wet_dry as wd
import inspect
vname = [n for n, obj in inspect.getmembers(wd) if not inspect.ismodule(obj) ]
# 

bd          = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',index_col=0,header=0,parse_dates=['birth_date','death_date'],low_memory=False,dtype=None)
date_names  = ['age cow','stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything  = pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',index_col=0,header=0,parse_dates=date_names, date_format='%m/%d/%Y',low_memory=False,dtype=None)
f           = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',parse_dates=['datex'])

dd          = bd['death_date']
fullday_datex = f['datex'].to_list()            #datex in list form
# f2cols      = [int(x) for x in f2.columns.tolist()] #list of WY_ids as integers


# last date in milk data or any other single date
datemax = f['datex'].max()  #timestamp
milking1 = f.iloc[-1:,1:]
milking2 = milking1.iloc[-1:,:].copy()    #grabs last row to determine all cows milking
milkingt = milking2.T
milkingmask = milkingt.values !=0       #mask to select milkers
milking1 = milkingt[milkingmask].index.tolist() 

milking = [int(x) for x in milking1]
# 
gone    = [idx for idx, x in dd.items() if x < datemax ]

alive   = [idx for idx, x in dd.items() if pd.isnull(x)]
# 
alivebutnotmilking = [x for x in alive if x not in milking]
# 
heif1 = iu.lb_first.loc[alivebutnotmilking] 
heif2 = heif1.loc[pd.isnull(heif1['1st_bdate']) |  (heif1['1st_bdate'] >= (datemax))]  #NOTE: using the | operator inside a list comp won't work (because the lists resulting from the two parts will be of differing lengths........)
# 
heifers = heif2.index.tolist()
dry = [x for x in alivebutnotmilking if x not in heifers]
# 
alive_count     = len(alive)
gone_count      = len(gone)
milking_count   = len(milking)
dry_count       = len(dry)
heifer_count    = len(heifers)

#check to see if lists are same
total = len(heifers) + len(dry) +len(milking)
alive_count == total
print('diff_check = ', len(bd) == len(alive)+len(gone))
print('len(bd)= ',len(bd), 'len(alive)= ', len(alive), 'len(gone)= ', len(gone))
print('len(heifers=)',len(heifers), '  len(dry)=', len(dry),'  len(milking)=', len(milking), '  total=', total)
# 
# iu.heifers.index.tolist() == heifers   
# 

# create df for last day of milking
status1 = pd.DataFrame({
    'milking':[ milking_count],
    'dry': [dry_count],
    'heifer': [heifer_count]  },
    index=[pd.to_datetime(datemax)])
   
status1.to_csv('F:\\Cows\\data\\status\\status1.csv')

milking = pd.DataFrame(milking,columns=['WY_id'])
dry     = pd.DataFrame(dry,columns=['WY_id'])
heifers = pd.DataFrame(heifers,columns=['WY_id'])
status2 = pd.concat([milking,dry,heifers],axis=1).fillna('')
status2.rename(columns={'WY_id': 'milking','WY_id': 'dry','WY_id': 'heifer'}, inplace=True)
# 
title = pd.to_datetime(datemax).date()
status2.index.name = title
status2.to_csv('F:\\Cows\\data\\status\\status2.csv')
# 
# create status column with M D H
statusm = 'M'
statusd = 'D'
statush = 'H'
milking['status']   = statusm
dry['status']       = statusd
heifers['status']   = statush
status_list = pd.concat([milking,dry,heifers],axis=0)
status_list.reset_index(drop=True,inplace=True)
status_list.sort_values(by=['status','WY_id'],ascending=[False,True],inplace=True)
status_list.to_csv('F:\\Cows\\data\\status\\status_column.csv')
