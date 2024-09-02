'''
rawmilkupdate.py
'''
import time

import datetime
from datetime import timedelta, datetime
import sys
import pandas as pd  
import numpy as np

from pyexcel_ods import get_data
from openpyxl.utils.datetime import to_excel

class HaltScriptException(Exception):
    pass

class RawMilkUpdate:
    def __init__(self):

        #Load data from LibreOffice Calc .ods file
        data = get_data('F:\\cows_LO\\daily_milk.ods')

        def convert_to_dataframe(sheet_data):
            df = pd.DataFrame(sheet_data)
            df = df.iloc[3:].reset_index(drop=True)
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            df.set_index(df.columns[0], inplace=True)
            df.index = df.index.fillna(0)
            df.index = df.index.astype(int)

            return df

        self.dmAM_liters1   = convert_to_dataframe(data['AM_liters'])
        self.dmAM_wy1       = convert_to_dataframe(data['AM_wy'])
        self.dmPM_liters1   = convert_to_dataframe(data['PM_liters'])
        self.dmPM_wy1       = convert_to_dataframe(data['PM_wy'])
        # self.stats          = convert_to_dataframe(data['stats'])
        
        self.dmAM_liters    = self.dmAM_liters1.iloc[:70, :]
        self.dmAM_wy        = self.dmAM_wy1.iloc[:70, :]
        self.dmPM_liters    = self.dmPM_liters1.iloc[:70, :]
        self.dmPM_wy        = self.dmPM_wy1.iloc[:70, :]
        #self.stats        = self.stats.iloc[]
        
     
     
        
        # self.dmAM_liters    = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_liters', skiprows=3,index_col=0,header=0)
        # self.dmAM_wy        = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_wy',     skiprows=3,index_col=0,header=0)
        # self.dmPM_liters    = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_liters', skiprows=3,index_col=0,header=0)
        # self.dmPM_wy        = pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='PM_wy',     skiprows=3,index_col=0,header=0)
        
        self.AM_liters      = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',          index_col=0,header=0)
        self.AM_wy          = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',              index_col=0,header=0)
        self.PM_liters      = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',          index_col=0,header=0)
        self.PM_wy          = pd.read_csv ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',              index_col=0,header=0)
    
        self.AM_lastdate    = int(self.AM_liters.columns[-1])
        self.dm_lastdate    = self.dmAM_liters.columns[-1] 
         
        self.gap    = (self.dm_lastdate - self.AM_lastdate)-1
        self.tdy = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print('AM liters:\n ',self.AM_liters.iloc[:2,-3:], '\n',
              'AM wy:\n',self.AM_wy.iloc[:2,-3:],'\n',
              'PM liters:\n ', self.PM_liters.iloc[:2,-3:],'\n',
              'PM wy:\n', self.PM_wy.iloc[:2,-3:])
        
        self.compare_dates()
        
       
        
        self.halt_script()
        
        self.amliters,  self.newdata_am_liters      = self.create_AM_liters()
        self.amwy,      self.newdata_AM_wy          = self.create_AM_wy()
        self.pmliters,  self.newdata_pm_liters      = self.create_PM_liters()
        self.pmwy,      self.newdata_PM_wy          = self.create_PM_wy()
        self.write_to_csv()   
        
    
    def compare_dates(self):
        print('last date in daily milk = ',self.dm_lastdate, 'last date in AM liters = ',self.AM_lastdate)    

        
    def halt_script(self):
        if self.dm_lastdate <= self.AM_lastdate:
            print('date not Ok, halting', self.dm_lastdate, self.AM_lastdate)
            raise HaltScriptException('Halting current module execution')
            # sys.exit(1) 
            
        elif self.dm_lastdate > self.AM_lastdate:
            print('date Ok, proceeding', 'start= ', self.AM_lastdate, 'end= ', self.dm_lastdate)
         
    
    
    def create_AM_liters(self):
        self.newdata_am_liters = self.dmAM_liters.loc[:, self.AM_lastdate+1:self.dm_lastdate].copy()
        self.amliters=pd.concat([self.AM_liters, self.newdata_am_liters],axis=1,join='inner')
        return self.amliters, self.newdata_am_liters

    def create_AM_wy(self):
        self.newdata_AM_wy = self.dmAM_wy.loc[:,self.AM_lastdate+1:self.dm_lastdate].copy()
        self.amwy=pd.concat([self.AM_wy, self.newdata_AM_wy],axis=1,join='inner')
        return self.amwy, self.newdata_AM_wy
        
    def create_PM_liters(self):
        self.newdata_PM_liters = self.dmPM_liters.loc[:,self.AM_lastdate+1:self.dm_lastdate].copy()
        
        # print('newdata \n', self.newdata_PM_liters.head(5),'\n') 
        # print('PM_liters: \n', self.PM_liters.iloc[:3,-5:])
        
        self.pmliters = pd.concat([self.PM_liters,self.newdata_PM_liters],axis=1,join='inner')
        
        # print('concat data, PM liters \n',self.pmliters.iloc[:3,-5:]),'\n'
        
        return self.pmliters, self.newdata_PM_liters
    
    def create_PM_wy(self):
        self.newdata_PM_wy = self.dmPM_wy.loc[:,self.AM_lastdate+1:self.dm_lastdate].copy()
        self.pmwy=pd.concat([self.PM_wy, self.newdata_PM_wy],axis=1,join='inner')
        return self.pmwy, self.newdata_PM_wy



    def write_to_csv(self):
        # print('hhhhhhh')
        self.amliters   .to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        self.amliters   .to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        self.amliters   .to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',mode='w',header=True,index=True)
        
        self.amwy       .to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        self.amwy       .to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        self.amwy       .to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',mode='w',header=True,index=True)
        
        self.pmliters   .to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        self.pmliters   .to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        self.pmliters   .to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',mode='w',header=True,index=True)
        
        self.pmwy       .to_csv(f"D:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        self.pmwy       .to_csv(f"E:\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        self.pmwy       .to_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',mode='w',header=True,index=True)

        
        