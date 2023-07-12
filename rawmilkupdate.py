import pandas as pd  
import numpy as np
import datetime
from datetime import timedelta, datetime
import sys
#
dmAM_liters=pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_liters', skiprows=2,index_col=0,header=0)
dmAM_wy=pd.read_excel     ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_wy',     skiprows=2,index_col=0,header=0)
dmPM_liters=pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_liters', skiprows=2,index_col=0,header=0)
dmPM_wy=pd.read_excel     ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_wy',     skiprows=2,index_col=0,header=0)
#
AM_liters=pd.read_csv   ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',          index_col=0,header=0)
AM_wy=pd.read_csv       ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',              index_col=0,header=0)
PM_liters=pd.read_csv   ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',          index_col=0,header=0)
PM_wy=pd.read_csv       ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',              index_col=0,header=0)
# 
now = datetime.now()
tdy =now.strftime("%Y_%m_%d %H_%M_%S")      #for the csv file names
#
dm_lastdate1 = dmAM_liters.columns[-1]
AM_lastdate = AM_liters.columns[-1]
# 
dm_str = dm_lastdate1.strftime('%Y-%m-%d')
AM_str = datetime.strptime(AM_lastdate,'%m/%d/%Y').strftime('%Y-%m-%d')
# 
if dm_str == AM_str:
    print(dm_str, AM_str)
    sys.exit()
#
#

# AM LITERS
dat1=AM_liters.columns[-1]                          #the date is a string eg '7/3/2023'
dat=datetime.strptime(dat1,'%m/%d/%Y').date()      #%H:%M:%S
# dat = dat2 + timedelta(days=1)
newdata=dmAM_liters.loc[:,dat:].copy()
newdata.columns = newdata.columns.strftime('%m/%d/%Y')
#
amliters=pd.concat([AM_liters,newdata],axis=1,join='inner')
amliters.replace(0,np.nan,inplace=True)
amliters.dropna(axis='columns', how='all', inplace=True)
#

# AM WY
newdata=dmAM_wy.loc[:,dat:].copy()
newdata.columns = newdata.columns.strftime('%m/%d/%Y')
#
amwy=pd.concat([AM_wy,newdata],axis=1,join='inner')
amwy.replace(0,np.nan,inplace=True)
amwy.dropna(axis='columns', how='all', inplace=True)
#

# PM LITERS
newdata=dmPM_liters.loc[:,dat:].copy()
newdata.columns = newdata.columns.strftime('%m/%d/%Y')
#
pmliters=pd.concat([PM_liters,newdata],axis=1,join='inner')
pmliters.replace(0,np.nan,inplace=True)
pmliters.dropna(axis='columns', how='all', inplace=True)
#

# PM WY
newdata=dmPM_wy.loc[:,dat:].copy()
newdata.columns = newdata.columns.strftime('%m/%d/%Y')
#
pmwy=pd.concat([PM_wy,newdata],axis=1,join='inner')
pmwy.replace(0,np.nan,inplace=True)
pmwy.dropna(axis='columns', how='all', inplace=True)
#

# write to csv
amliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{tdy}.csv")
amliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{tdy}.csv")
amliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',mode='w',header=True,index=True)
# 
amwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{tdy}.csv")
amwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{tdy}.csv")
amwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',mode='w',header=True,index=True)
# 
pmliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{tdy}.csv")
pmliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{tdy}.csv")
pmliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',mode='w',header=True,index=True)
# 
pmwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{tdy}.csv")
pmwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{tdy}.csv")
pmwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',mode='w',header=True,index=True)

