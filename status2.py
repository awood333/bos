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
# 
date_names = ['age cow','stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything=pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',index_col=0,header=0,parse_dates=date_names, date_format='%m/%d/%Y',low_memory=False,dtype=None)
# 
f=pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',parse_dates=['datex'])
f1 = f.iloc[:,:-5].copy()       ##drops the last 5 columns - leaving datex and the WY_ids
f2 = f.iloc[:,1:-5].copy()      #drops the datex and the last 5 columns - leaving only the WY_ids
# 
dd      = bd['death_date']
fullday_datex = f['datex'].to_list()            #datex in list form
f2cols  = [int(x) for x in f2.columns.tolist()] #list of WY_ids as integers
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
datex = fx['datex']
# 
col_integers = [int(x) for x in fx2.columns.to_list()]
cols =col_integers          #200+ cols WY_ids
rows = len(fx['datex'])     #30 rows    dates
# 
# maskmilk1 = []
maskmilk2 = []
# maskheif1 = []
maskheif2  = []
# 
# Loop through each element in fullday_wy
for i in cols:
    maskmilk1 = []
    maskheif1 = [] 
    # 
    for j in range(rows):
        r       = fx.iloc[j,i]
        calf1   = iu.lb3a.iloc[i,1]
        daynum  = datex.iloc[j]
        # 
        maskmilk = r>0
        maskheif = ((daynum < calf1) |( pd.isnull(calf1)))
        maskmilk1.append(maskmilk)
        maskheif1.append(maskheif)
        j +=1
        # 
    maskmilk2.append(maskmilk1) 
    maskheif2.append(maskheif1)  

# 
milking = pd.DataFrame(maskmilk2).T
heifers = pd.DataFrame(maskheif2)