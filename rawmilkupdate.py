import pandas as pd  
import numpy as np
import datetime
from datetime import timedelta, datetime
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

# AM LITERS
dat1=AM_liters.columns[-1]
dat2=pd.to_datetime(dat1,format="%m/%d/%Y")
# dat2=datetime.strptime(dat1,'%Y-%m-%d %H:%M:%S')

dat = dat2 + timedelta(days=1)
tdy=str(datetime.now().strftime("%m_%d_%Y"))
newdata=dmAM_liters.loc[:,dat:].copy()
#
amliters=pd.concat([AM_liters,newdata],axis=1,join='inner')
amliters.replace(0,np.nan,inplace=True)
#
amliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{tdy}.csv")
amliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{tdy}.csv")
amliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',mode='w',index=True)
# NOTE this to_csv command does not overwrite the last column of the df - so, if the wrong data has gotten written in, you gotta go erase that col 

# AM WY
# dat1=AM_liters.columns[-1]
# dat2=datetime.strptime(dat1,'%Y-%m-%d %H:%M:%S')
# dat = dat2 + timedelta(days=1)
# tdy=str(datetime.now().strftime("%m_%d_%Y"))
newdata=dmAM_wy.loc[:,dat:].copy()
#
amwy=pd.concat([AM_wy,newdata],axis=1,join='inner')
amwy.replace(0,np.nan,inplace=True)
#
amwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{tdy}.csv")
amwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{tdy}.csv")
amwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',mode='w',index=True)
#

# PM LITERS
# dat1=AM_liters.columns[-1]
# dat2=datetime.strptime(dat1,'%Y-%m-%d %H:%M:%S')
# dat = dat2 + timedelta(days=1)
# tdy=str(datetime.now().strftime("%m_%d_%Y"))
newdata=dmPM_liters.loc[:,dat:].copy()
#
pmliters=pd.concat([PM_liters,newdata],axis=1,join='inner')
pmliters.replace(0,np.nan,inplace=True)
#
pmliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{tdy}.csv")
pmliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{tdy}.csv")
pmliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',mode='w',index=True)

# PM WY
# dat1=AM_liters.columns[-1]
# dat2=datetime.strptime(dat1,'%Y-%m-%d %H:%M:%S')
# dat = dat2 + timedelta(days=1)
# tdy=str(datetime.now().strftime("%m_%d_%Y"))
newdata=dmPM_wy.loc[:,dat:].copy()
#
pmwy=pd.concat([PM_wy,newdata],axis=1,join='inner')
pmwy.replace(0,np.nan,inplace=True)
#
pmwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{tdy}.csv")
pmwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{tdy}.csv")
pmwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',mode='w',index=True)

