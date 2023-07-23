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

bd=pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',index_col=0,header=0,parse_dates=['birth_date','death_date'],low_memory=False,dtype=None)
date_names = ['age cow','stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything=pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',index_col=0,header=0,parse_dates=date_names, date_format='%m/%d/%Y',low_memory=False,dtype=None)
f=pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',parse_dates=['datex'])
f1 = f.iloc[:,:-5].copy()       ##drops the last 5 columns - leaving datex and the WY_ids
f2 = f.iloc[:,1:-5].copy()      #drops the datex and the last 5 columns - leaving only the WY_ids
# 
dd      = bd['death_date']
fullday_datex = f['datex'].to_list()            #datex in list form
f2cols  = [int(x) for x in f2.columns.tolist()] #list of WY_ids as integers
# 

# creates df for last date in milk data
datemax = f['datex'].max()  #timestamp
milking1 = f2.iloc[-1:,:]
milking2 = milking1.iloc[-1:,:].copy()    
milkingt = milking2.T
milkingmask = milkingt.values !=0
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
len(bd) == len(alive)+len(gone)
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

# 
# creates df for the past whole month with datex intact and a 'day' index.
# 
month_filter = 6
year_filter  = 2023
# 
fx = f.loc[    (
        (f['year'] == year_filter) & 
        (f['month'] == month_filter)
        ),    :    ].reset_index(drop=True)
# 
fx2 = fx.iloc[:,1:-5]

#  Loop
datex = fx['datex']
# 
col_integers = [int(x) for x in fx2.columns.to_list()]
cols =col_integers          #200+ cols WY_ids
rows = len(fx['datex'])     #30 rows    dates
# 
maskalive2  = []
maskmilk2   = []
maskheif2   = []
alivenotmilking2 = []
gottabedry2 = []
gone2       = []

# 
# Loop through each element in fullday_wy
for i in cols:                              # wy index
    maskmilk1   = []
    maskheif1   = [] 
    maskalive1  = []
    alivenotmilking1 = []
    gottabedry1 = []
    gone1       = []
    
    # 
    for j in range(rows):                   #date index
        r       = fx.iloc[j,i]
        calf1   = iu.lb_first.iloc[i,1]
        daynum  = datex.iloc[j]
        deathdate = dd.iloc[i]
        # 
        maskmilk        = r>0
        maskheif        = (((daynum < calf1)        |( pd.isnull(calf1))) 
                            & ((daynum < deathdate) | (pd.isnull(deathdate))   ) 
                                    )
        maskalive       = ((daynum < deathdate)     |  (pd.isnull(deathdate)))
        alivenotmilking = ( ((daynum < deathdate)   |  (pd.isnull(deathdate))) & (r==0))
        gottabedry      = ( ((daynum < deathdate)   |  (pd.isnull(deathdate)))
                            & (r==0)  
                            & ((daynum > calf1)     & ( pd.notnull(calf1))) 
                            )
        gone            = ((daynum >= deathdate)    |  (pd.notnull(deathdate)))
                # 
        maskmilk1.  append(maskmilk)
        maskheif1.  append(maskheif)
        maskalive1. append(maskalive)
        alivenotmilking1.append(alivenotmilking)
        gottabedry1.append(gottabedry)
        gone1.      append(gone)
        
        j +=1
        # 
        
    maskalive2.     append(maskalive1)
    maskmilk2.      append(maskmilk1) 
    maskheif2.      append(maskheif1) 
    alivenotmilking2.append(alivenotmilking1)
    gottabedry2.    append(gottabedry1)
    gone2.          append(gone1)
    #  
    # 
milking = pd.DataFrame(maskmilk2)
heifers = pd.DataFrame(maskheif2)
heif_dry= pd.DataFrame(alivenotmilking2)
dry     = pd.DataFrame(gottabedry2)
alive   = pd.DataFrame(maskalive2)
gone    = pd.DataFrame(gone2)
# 
milking_count =     milking .sum(axis=0)
heifer_count =      heifers .sum(axis=0)
heif_dry_count =    heif_dry.sum(axis=0)
dry_count =         dry     .sum(axis=0)
alive_count=        alive   .sum(axis=0)
gone_count =        gone    .sum(axis=0)
total_check =       alive_count + gone_count

status3 = pd.DataFrame({
    'milking'   :milking_count,
    'dry'       :dry_count,
    'heif+dry'  :heif_dry_count,
    'heifers'   :heifer_count,
    'alive'     :alive_count,
    'gone'      :gone_count,
    'total check':total_check
    })
status3.to_csv('F:\\Cows\\data\\status\\status3.csv')
# 

xx = dry.iloc[:,-1:]
mask = xx>0
mask

alivecomp   = [x for x in alive.iloc[:,-1:] if x not in iu.alivemask]
gonecomp    = [x for x in gone.iloc[:,-1:]  if x not in iu.deadmask]