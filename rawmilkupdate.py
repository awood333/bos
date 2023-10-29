'''
rawmilkupdate.py
'''
import pandas as pd  
import numpy as np
import datetime
from datetime import timedelta, datetime
import sys

class RawMilkUpdate:
    def __init__(self):
        
        self.dmAM_liters    = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_liters', skiprows=2,index_col=0,header=0)
        self.dmAM_wy        = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_wy',     skiprows=2,index_col=0,header=0)
        self.dmPM_liters    = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_liters', skiprows=2,index_col=0,header=0)
        self.dmPM_wy        = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_wy',     skiprows=2,index_col=0,header=0)
        
        self.AM_liters      = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',          index_col=0,header=0)
        self.AM_wy          = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',              index_col=0,header=0)
        self.PM_liters      = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',          index_col=0,header=0)
        self.PM_wy          = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',              index_col=0,header=0)
   
        self.now            = datetime.now()
        self.tdy            = self.now.strftime("%Y_%m_%d %H_%M_%S")      #for the csv file names
 
        self.dm_lastdate1   = self.dmAM_liters.columns[-1]
        self.AM_lastdate    = self.AM_liters.columns[-1]
  
        self.dm_str              = self.dm_lastdate1.strftime('%Y-%m-%d')
        self.AM_str              = datetime.strptime(self.AM_lastdate,'%m/%d/%Y').strftime('%Y-%m-%d')
        
        self.compare_dates()
        self.halt_script()
        self.amliters       = self.create_AM_liters()
        self.amwy           = self.create_AM_wy()
        self.pmliters       = self.create_PM_liters()
        self.pmwy           = self.create_PM_wy()
        self.write_to_csv()   
        
        
        
        
    def compare_dates(self):
        print('dm_str = ',self.dm_str, 'AM_str = ',self.AM_str)    

        
    def halt_script(self):
        if self.dm_str <= self.AM_str:
            print('date not Ok, halting', self.dm_str, self.AM_str)
            sys.exit(1) 
            
        elif self.dm_str > self.AM_str:
            print('date Ok, proceeding', self.dm_str, self.AM_str)
        

    def create_AM_liters(self):
        dat1=self.AM_liters.columns[-1]                          #the date is a string eg '7/3/2023'
        dat1=datetime.strptime(dat1,'%m/%d/%Y').date()      #%H:%M:%S
        dat = dat1 + timedelta(days=1)

        newdata = self.dmAM_liters.loc[:,dat:].copy()
        newdata.columns = newdata.columns.strftime('%m/%d/%Y')
        
        amliters=pd.concat([self.AM_liters,newdata],axis=1,join='inner')
        amliters.replace(0,np.nan,inplace=True)
        amliters.dropna(axis='columns', how='all', inplace=True)
        print(amliters.iloc[:1,-5:])
        return amliters

    def create_AM_wy(self):
        dat1=self.AM_wy.columns[-1]                          #the date is a string eg '7/3/2023'
        dat1=datetime.strptime(dat1,'%m/%d/%Y').date()      #%H:%M:%S
        dat = dat1 + timedelta(days=1)

        newdata = self.dmAM_wy.loc[:,dat:].copy()
        newdata.columns = newdata.columns.strftime('%m/%d/%Y')

        amwy=pd.concat([self.AM_wy,newdata],axis=1,join='inner')
        amwy.replace(0,np.nan,inplace=True)
        amwy.dropna(axis='columns', how='all', inplace=True)
        return amwy
        
#

    def create_PM_liters(self):
        dat1 = self.PM_liters.columns[-1]
        dat1=datetime.strptime(dat1,'%m/%d/%Y').date()
        dat = dat1 + timedelta(days=1)    
            
        newdata = self.dmPM_liters.loc[:,dat:].copy()
        newdata.columns = newdata.columns.strftime('%m/%d/%Y')

        pmliters = pd.concat([self.PM_liters,newdata],axis=1,join='inner')
        pmliters.replace(0,np.nan,inplace=True)
        pmliters.dropna(axis='columns', how='all', inplace=True)
        return pmliters
    #

    def create_PM_wy(self):
        dat1 = self.PM_wy.columns[-1]
        dat1=datetime.strptime(dat1,'%m/%d/%Y').date()
        dat = dat1 + timedelta(days=1) 
           
        newdata = self.dmPM_wy.loc[:,dat:].copy()
        newdata.columns = newdata.columns.strftime('%m/%d/%Y')
     
        pmwy=pd.concat([self.PM_wy,newdata],axis=1,join='inner')
        pmwy.replace(0,np.nan,inplace=True)
        pmwy.dropna(axis='columns', how='all', inplace=True)
        return pmwy
      

    def write_to_csv(self):
        self.amliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        self.amliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        self.amliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',mode='w',header=True,index=True)
        # # 
        self.amwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        self.amwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        self.amwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',mode='w',header=True,index=True)
        # 
        self.pmliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        self.pmliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        self.pmliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',mode='w',header=True,index=True)
        # 
        self.pmwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        self.pmwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        self.pmwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',mode='w',header=True,index=True)

