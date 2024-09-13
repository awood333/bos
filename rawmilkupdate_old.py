'''
rawmilkupdate.py
'''

import datetime
from datetime import timedelta, datetime
import sys
import pandas as pd  
import numpy as np

from pyexcel_ods import get_data
from openpyxl.utils.datetime import to_excel
class RawMilkUpdate:
    def __init__(self):

# Load data from LibreOffice Calc .ods file
        data = get_data('F:\\cows_LO\\daily_milk.ods')

        def convert_to_dataframe(sheet_data):
            df = pd.DataFrame(sheet_data)
            
            df = df.iloc[2:].reset_index(drop=True)
            
            df.columns = df.iloc[0]
            
            df = df[1:].reset_index(drop=True)
             
            df.set_index(df.columns[0], inplace=True)
             
            df.columns = pd.to_datetime(df.columns, format='%m/%d/%Y', errors='coerce')  #.strftime('%m/%d/%Y')
            return df

        # Convert each sheet's data to a DataFrame
        self.dmAM_liters1   = convert_to_dataframe(data['AM_liters'])
        self.dmAM_wy1       = convert_to_dataframe(data['AM_wy'])
        self.dmPM_liters1   = convert_to_dataframe(data['PM_liters'])
        self.dmPM_wy1       = convert_to_dataframe(data['PM_wy'])
        self.stats          = convert_to_dataframe(data['stats'])
        
        self.dmAM_liters    = self.dmAM_liters1.iloc[:70, :]
        self.dmAM_wy        = self.dmAM_wy1.iloc[:70, :]
        self.dmPM_liters    = self.dmPM_liters1.iloc[:70, :]
        self.dmPM_wy        = self.dmPM_wy1.iloc[:70, :]
        # self.stats        = self.stats.iloc[]
        
     
     
        
        # self.dmAM_liters    = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_liters', skiprows=2,index_col=0,header=0)
        # self.dmAM_wy        = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_wy',     skiprows=2,index_col=0,header=0)
        # self.dmPM_liters    = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_liters', skiprows=2,index_col=0,header=0)
        # self.dmPM_wy        = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_wy',     skiprows=2,index_col=0,header=0)
        
        self.AM_liters      = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',          index_col=0,header=0)
        self.AM_wy          = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',              index_col=0,header=0)
        self.PM_liters      = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',          index_col=0,header=0)
        self.PM_wy          = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',              index_col=0,header=0)
   
        self.now            = datetime.now()
        self.tdy           = 
    
        self.AM_lastdate_str    = self.AM_liters.columns[-1]    #string
        self.AM_lastdate_ts    = pd.to_datetime(self.AM_liters.columns[-1])    # convert string to timestamp
        self.AM_lastdate_dt      = datetime.strptime(self.AM_lastdate_str, "%m/%d/%Y").date()
        self.AM_lastdate_dt_adj    = self.AM_lastdate_dt + timedelta (days=1)   #datetime
        
        
        self.dm_lastdate_ts    = self.dmAM_liters.columns[-1]  # timestamp   
        self.dm_lastdate_dt = self.dmAM_liters.columns[-1].to_pydatetime()
         
        
        self.compare_dates()
        self.halt_script()
        self.amliters       = self.create_AM_liters()
        self.amwy           = self.create_AM_wy()
        self.pmliters       = self.create_PM_liters()
        self.pmwy           = self.create_PM_wy()
        self.write_to_csv()   
        
    
    def compare_dates(self):
        print('last date in daily milk = ',self.dm_lastdate_ts, 'last date in AM liters = ',self.AM_lastdate_ts)    

        
    def halt_script(self):
        if self.dm_lastdate_ts <= self.AM_lastdate_ts:
            print('date not Ok, halting', self.dm_lastdate_ts, self.AM_lastdate_ts)
            sys.exit(1) 
            
        elif self.dm_lastdate_ts > self.AM_lastdate_ts:
            print('date Ok, proceeding', 'start= ', self.AM_lastdate_ts, 'end= ', self.dm_lastdate_ts)
         

    def create_AM_liters(self):
        
        newdata = self.dmAM_liters.loc[:, self.AM_lastdate_dt_adj:self.dm_lastdate_dt].copy()
        newdata.columns = [col.strftime('%m/%d/%Y') for col in newdata.columns]
        amliters=pd.concat([self.AM_liters,newdata],axis=1,join='inner')
        # amliters.replace(0,np.nan,inplace=True)
        # amliters.dropna(axis='columns', how='all', inplace=True)
       
        return amliters

    def create_AM_wy(self):

        newdata = self.dmAM_wy.loc[:,self.AM_lastdate_dt_adj:self.dm_lastdate_dt].copy()
        newdata.columns = [col.strftime('%m/%d/%Y') for col in newdata.columns]

        amwy=pd.concat([self.AM_wy,newdata],axis=1,join='inner')
        # amwy.replace(0,np.nan,inplace=True)
        # amwy.dropna(axis='columns', how='all', inplace=True)
        return amwy
        

    def create_PM_liters(self):

        newdata = self.dmPM_liters.loc[:,self.AM_lastdate_dt_adj:self.dm_lastdate_dt].copy()
        newdata.columns = [col.strftime('%m/%d/%Y') for col in newdata.columns]


        pmliters = pd.concat([self.PM_liters,newdata],axis=1,join='inner')
        # pmliters.replace(0,np.nan,inplace=True)
        # pmliters.dropna(axis='columns', how='all', inplace=True)
        return pmliters
    
    
    def create_PM_wy(self):

        newdata = self.dmPM_wy.loc[:,self.AM_lastdate_dt_adj:self.dm_lastdate_dt].copy()
        newdata.columns = [col.strftime('%m/%d/%Y') for col in newdata.columns]

 
        pmwy=pd.concat([self.PM_wy,newdata],axis=1,join='inner')
        # pmwy.replace(0,np.nan,inplace=True)
        # pmwy.dropna(axis='columns', how='all', inplace=True)
        return pmwy
      

    def write_to_csv(self):
        print('fuku')
        # self.amliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        # self.amliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        # self.amliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',mode='w',header=True,index=True)
        # # # 
        # self.amwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        # self.amwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        # self.amwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',mode='w',header=True,index=True)
        # # 
        # self.pmliters.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        # self.pmliters.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        # self.pmliters.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',mode='w',header=True,index=True)
        # # 
        # self.pmwy.to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        # self.pmwy.to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        # self.pmwy.to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',mode='w',header=True,index=True)
        