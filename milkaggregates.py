import pandas as pd
import numpy as np
import datetime 
import openpyxl
# import rawmilkupdate as rm
# import birthdeath as bd
# import insem_ultra as iu 
#
AM_liters = pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',   index_col=0,header=0)
AM_wy   = pd.read_csv       ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',       index_col=0,header=0)
PM_liters = pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',   index_col=0,header=0)
PM_wy   = pd.read_csv       ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',       index_col=0,header=0)
bd      = pd.read_csv       ('F:\\COWS\\data\\csv_files\\birth_death.csv',                       header=0)
all     = pd.read_csv       ('F:\\COWS\\data\\insem_data\\all.csv',                              header=0)
status  = pd.read_csv     ('F:\\COWS\\data\status\\status_column.csv',            index_col=0,header=0)       
# 
liters1_am  = AM_liters
wy1_am      = AM_wy
liters1_pm  = PM_liters
wy1_pm      = PM_wy
# 
wy      = bd['WY_id']
alive1  = bd['death_date'].isnull()
alive   = wy.loc[alive1].copy()
alive.reset_index(drop=True,inplace=True)
#
wy_am =     wy1_am.       fillna(0).copy()
wy_pm =     wy1_pm.       fillna(0).copy()
liters_am = liters1_am.   fillna(0).copy()
liters_pm = liters1_pm.   fillna(0).copy()
#
datex       = pd.Series(list(liters_am.columns.values ))
datex.name  = 'datex'
#
maxcols     = len(datex)             #1575                          #len of dates (col headers for liters - which starts with 'start_date')
maxrows     = len(wy)   #201                          #len of groupx - will accomodate new calves - continuous series from 1~200 will be the output col heading 
idx_cols1   = [*range(1,maxcols+1)]         #list
#
idx     = np.zeros((maxrows,maxcols))                   #creates basic array of zeros
idx_am  = idx.copy()
idx_pm  = idx.copy()
#
# Numpyzation
wy_am1 =    wy_am.      to_numpy(dtype=int)
wy_pm1 =    wy_pm.      to_numpy(dtype=int)
liters_am1= liters_am.  to_numpy(dtype=float)
liters_pm1= liters_pm.  to_numpy(dtype=float)



#   AM calc
#
rows = np.array(range(0, 70))  
cols = np.array(range(0, len(datex)))  # Convert to NumPy array
# 
w = wy_am1[rows    [:, np.newaxis],    cols]     #array 70,1813
l = liters_am1[rows[:, np.newaxis], cols]        #array 70,1813
idx_am[w,cols] = l
# 
#   PM calc
#
w = wy_pm1[rows    [:, np.newaxis],    cols]     #array 70,1813
l = liters_pm1[rows[:, np.newaxis], cols]        #array 70,1813
idx_pm[w,cols] = l
        
#
am2 = pd.DataFrame(idx_am)
am2.columns=datex
am = am2.T
am.replace(0,np.nan,inplace=True)
am.index=pd.to_datetime(am.index)
am.drop(am.iloc[:,0:1],axis=1,inplace=True)

#
pm2 = pd.DataFrame(idx_pm)
pm2.columns=datex
pm = pm2.T
pm.replace(0,np.nan,inplace=True)
pm.index = pd.to_datetime(pm.index)
pm.drop(pm.iloc[:,0:1],axis=1,inplace=True)

#
fullday1 = np.add(idx_am,idx_pm)
fullday2 = pd.DataFrame(fullday1)
fullday2.columns = datex
fullday = fullday2.T
fullday.index.name = 'datex'
fullday.replace(0,np.nan,inplace=True)
fullday.index=pd.to_datetime(fullday.index).date
fullday.drop(fullday.iloc[:,0:1],axis=1,inplace=True)
fullday.index.name = 'datex'

# 10 day
#
lastday = fullday.iloc[-1:,:]     #last milking day recorded
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
tenday = tendayT
avg = tenday.mean(axis=1).astype(float)
tendayT['avg']=avg.round(1) 
# 
tenday.index.name='WY_id'
# 
sumx = tenday.sum(axis=0).astype(float)
# tenday.loc[''] = sumx.round(0)                   # [''] means 'empty row'
tenday.loc['total'] = tenday.sum(axis=0)
# 
allcols = ['WY_id','age last calf','i_date','age last insem','u_date','readex','days left']
all1    = all.loc[:,allcols].copy()
all2    = all1.merge(status,how='left',left_on='WY_id',right_on='WY_id')
 
# all2    = all1.merge(status['status'],how='left',left_on='WY_id',right_on='WY_id')

# sum and nonzero count for entire milk df
#
milk = fullday
fullday.replace(np.nan,0,inplace=True)
#
milkrowcount =   milk.astype(bool).sum(axis=1)
milkrowsum =     milk.sum(axis=1,skipna=True)    #sum for that day, all cows
milkcolsum =     milk.sum(axis=0,skipna=True)    #sum for that cow for all days
milk['avg'] =    milkrowsum                      #blank col sets up the group agg
milk['count'] =  milkrowcount
milk.index =datex
milk.index.name = 'datex'
milk.index =pd.to_datetime(milk.index)

# monthly and weekly
#

milk['year']=milk.index.year
milk['month']=milk.index.month
milk['week']=milk.index.isocalendar().week
#  the as_index=False leaves the new columns accessible for .loc, otherwise they become part of a multi-index
milk_monthly_sum   =   milk.groupby(['year','month'],          as_index=False).sum()    
milk_monthly_mean1  =   milk.groupby(['year','month'],          as_index=False).mean()     
weekly         =   milk.groupby(['year','month','week'],   as_index=False).mean() 
# 
# 
# change names because 'sum' will eventually mean the monthly total vs the avg
milk_monthly_mean1.rename(columns={'avg':'avg sum','count':'avg count'},inplace=True)
# cuts out the middle cols
monthly1       = milk_monthly_mean1.iloc[-12:,[0,1,-2,-3]].copy()
# 
monthly1['total'] = milk_monthly_sum['avg']
# 
def format_num(num):
    return '{:,.0f}'.format(num)
# 
monthly1[['avg count', 'avg sum', 'total']] = monthly1[['avg count', 'avg sum', 'total']].applymap(format_num)
monthly = monthly1.reset_index(drop=True)



# WRITE TO CSV
#
am.to_csv('F:\\COWS\data\\milk_data\\halfday\\am\\am.csv')
pm.to_csv('F:\\COWS\data\\milk_data\\halfday\\pm\\pm.csv')
# 
fullday_lastdate = pd.DataFrame(index=[fullday.index[-1]], columns=['last_date'])

# 
fullday.to_csv('F:\\COWS\data\\milk_data\\fullday\\fullday.csv')
fullday_lastdate.to_csv('F:\\COWS\data\\milk_data\\fullday\\fullday_lastdate.csv')
# 
weekly.         to_csv('F:\\COWS\data\\milk_data\\totals\\weekly.csv')
monthly.        to_csv('F:\\COWS\data\\milk_data\\totals\\monthly.csv')
#
milk.           to_csv('F:\\COWS\data\\milk_data\\fullday\\milk.csv')
tenday.         to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day.csv')
tenday1.        to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day1.csv')
all2.           to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day2.csv')
# 
with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\output.xlsx') as writer:
    tenday.      to_excel(writer, sheet_name='tenday')
    tenday1.     to_excel(writer, sheet_name='tenday2')
    all2.        to_excel(writer, sheet_name='all')
# 
print(tenday.iloc[:1,:])
