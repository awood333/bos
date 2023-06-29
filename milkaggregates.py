import pandas as pd
import numpy as np
import datetime 
import rawmilkupdate as rm
import birthdeath as bd
import insem_ultra as iu 
#
liters1_am  = rm.amliters
wy1_am      = rm.amwy
liters1_pm  = rm.pmliters
wy1_pm      = rm.pmwy
all3        = iu.all
#
wy=(bd.WY_id)
alive1=bd.ddate.isnull()
alive=wy.loc[alive1].copy()
alive.reset_index(drop=True,inplace=True)
#
wy_am=wy1_am.           fillna(0).copy()
wy_pm=wy1_pm.           fillna(0).copy()
liters_am=liters1_am.   fillna(0).copy()
liters_pm=liters1_pm.   fillna(0).copy()
#
datex=pd.Series(list(liters_am.columns.values ))
datex.name = 'datex'
#
maxcols=len(datex)             #1575                          #len of dates (col headers for liters - which starts with 'start_date')
maxrows= len(wy)   #201                          #len of groupx - will accomodate new calves - continuous series from 1~200 will be the output col heading 
idx_cols1=[*range(1,maxcols+1)]         #list
#
idx=np.zeros((maxrows,maxcols))                   #creates basic array of zeros
idx_am=idx.copy()
idx_pm=idx.copy()
#
wy_am1=wy_am.to_numpy(dtype=int)
wy_pm1=wy_pm.to_numpy(dtype=int)
liters_am1=liters_am.to_numpy(dtype=float)
liters_pm1=liters_pm.to_numpy(dtype=float)

#   FULL DAY
#
rows=wy_am.index                 
cols=[*range(1,len(datex))]     

#   AM calc
#
for j in cols:
    for i in rows-1:
        w=wy_am1[i,j]
        l=liters_am1[i,j]
        idx_am[w,j]=l

#   PM calc
#
for j in cols:
    for i in rows-1:
        w=wy_pm1[i,j]
        l=liters_pm1[i,j]
        idx_pm[w,j]=l 
        
        
#
am2=pd.DataFrame(idx_am)
am2.columns=datex
am=am2.T
am.replace(0,np.nan,inplace=True)
am.index=pd.to_datetime(am.index)
am.drop(am.iloc[:,0:1],axis=1,inplace=True)
am.to_csv('F:\\COWS\data\\milk_data\\halfday\\am\\am.csv')
#
pm2=pd.DataFrame(idx_pm)
pm2.columns=datex
pm=pm2.T
pm.replace(0,np.nan,inplace=True)
pm.index=pd.to_datetime(pm.index)
pm.drop(pm.iloc[:,0:1],axis=1,inplace=True)
pm.to_csv('F:\\COWS\data\\milk_data\\halfday\\pm\\pm.csv')
#
fullday1=np.add(idx_am,idx_pm)
fullday2=pd.DataFrame(fullday1)
fullday2.columns=datex
fullday=fullday2.T
fullday.replace(0,np.nan,inplace=True)
fullday.index=pd.to_datetime(fullday.index)
fullday.drop(fullday.iloc[:,0:1],axis=1,inplace=True)
fullday.to_csv('F:\\COWS\data\\milk_data\\fullday\\fullday.csv')
# 10 day
#
lastday=fullday.iloc[-1:,:]     #last milking day recorded
ld=lastday.loc[:,(lastday>0).any()].columns.tolist()     #the .any is important
# ld_nums1 = ld[:] #this drops the final 5 cols (avg, count, year.month,week) which are strings
ld_nums = [int(x-1) for x in ld] #decrements the wy's
# 
tenday1 = fullday.iloc[-10:,:].copy() # has all wy's
tenday2 = tenday1.loc[:,ld]           # has milkers only
tenday3 = tenday1.iloc[:,ld_nums]      #unnecessary?
# 
tendayT=tenday2.T
# 
tendayT.columns=[1,2,3,4,5,6,7,8,9,10]
tenday=tendayT
avg = tenday.mean(axis=1).astype(float)
tendayT['avg']=avg.round(1) 
# 
tenday.index.name='WY_id'
# 
sumx = tenday.sum(axis=0).astype(float)
# tenday.loc[''] = sumx.round(0)                   # [''] means 'empty row'

tenday.loc['total'] = tenday.sum(axis=0)

all3cols=['age lastcalf bdate','i_date','age last insem','u_date','readex','days left']
all4= all3.loc[:,all3cols].copy()

# sum and nonzero count for entire milk df
#
milk=fullday
fullday.replace(np.nan,0,inplace=True)
#
milkrowcount=   milk.astype(bool).sum(axis=1)
milkrowsum=     milk.sum(axis=1,skipna=True)
milkcolsum=     milk.sum(axis=0,skipna=True)
milk['avg']=    milkrowsum
milk['count']=  milkrowcount
milk.index=datex
milk.index.name = 'datex'
milk.index=pd.to_datetime(milk.index)

# monthly and weekly
#
milk['year']=milk.index.year
milk['month']=milk.index.month
milk['week']=milk.index.isocalendar().week
#  the as_index=False leaves the new columns accessible for .loc, otherwise they become part of a multi-index
milk_monthly=   milk.groupby(['year','month'],          as_index=False).mean()     
milk_weekly=    milk.groupby(['year','month','week'],   as_index=False).mean() 
#
milk_monthly.   set_index(['year','month'],             drop=True,inplace=True)
milk_weekly.    set_index(['year','month','week'],      drop=True,inplace=True)
#
milk_monthly.   drop(milk_monthly.iloc[:,0:-3]  ,       axis=1,inplace=True)
milk_monthly.   drop(milk_monthly.iloc[:,-1:]  ,        axis=1,inplace=True)        #gets rid of 'week'
#
milk_weekly=    milk.groupby(['year','month','week'],   as_index=False).mean() 
milk_weekly.    drop(milk_weekly .iloc[:,0:-2]   ,      axis=1,inplace=True)
#
monthly =   milk_monthly.iloc[-12:,:]
weekly  =   milk_weekly.iloc[-26:,:]

# WRITE TO CSV
#
weekly.         to_csv('F:\\COWS\data\\milk_data\\totals\\weekly.csv')
monthly.        to_csv('F:\\COWS\data\\milk_data\\totals\\monthly.csv')
#
milk.           to_csv('F:\\COWS\data\\milk_data\\fullday\\milk.csv')
tenday.         to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day.csv')
tenday1.        to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day1.csv')
all4.           to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day2.csv')



