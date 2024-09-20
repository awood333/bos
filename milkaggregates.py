import time
import pandas as pd
import numpy as np
from datetime import datetime

import subprocess
import pyexcel_io
import os

from RemoteFilesaveUtils import RemoteFilesaveUtils as rfu
from concurrent.futures import ThreadPoolExecutor

class MilkAggregates:
    def __init__(self):
        
        self.paths = [
            ('fullday', 'fullday\\fullday.csv'),
            ('tenday', 'totals\\milk_aggregates\\tenday.csv'),
            ('tenday1', 'totals/milk_aggregates/tenday1.csv'),
        ]
        
        self.bd      = pd.read_csv       ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date', 'death_date'])
        self.lag     = -10
        print('lag = ', self.lag)
        
        self.date_format='%m/%d/%Y'
        
        [self.LBP, self.RBP]     = self.create_basepath()
        
        self.basics()
        
        [
        self.fullday, self.fullday_xl,
        self.fullday_lastdate]          = self.fullday_calc()
        
        self.tenday, self.tenday1           = self.ten_day()
        self.milk                           = self.create_avg_count()
        
        [
        self.monthly, 
        self.monthly_sum, 
        self.monthly_mean, 
        self.weekly_sum, 
        self.weekly_mean]           = self.create_monthly_weekly()

        # self.write_to_csv()
 

    
    def create_basepath(self):

        if os.name == 'nt':  # Windows
            self.LBP = 'F:\\COWS\\data\\milk_data\\'
            self.RBP = 'Z:/My Drive/COWS/data/milk_data'
        elif os.name == 'posix':  # Linux
            self.LBP = '/home/alanw/data/milk_data'
            self.RBP = 'gdrive:My Drive/COWS/data/milk_data'

        return self.LBP, self.RBP
    


    def basics(self):       

        self.AM_liters = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',
                                          index_col=0)
        self.AM_wy     = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',
                                          index_col=0)
        self.PM_liters = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',
                                          index_col=0)
        self.PM_wy     = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',
                                          index_col=0)
       
        self.wy      = self.bd['WY_id']
        self.alive1  = self.bd['death_date'].isnull()
        self.alive   = self.wy.loc[self.alive1].copy()
        self.alive.reset_index(drop=True,inplace=True)

        self.liters_am  = self.AM_liters .iloc[:, self.lag:].copy()
        self.wy_am      = self.AM_wy     .iloc[:, self.lag:].copy()
        self.liters_pm  = self.PM_liters .iloc[:, self.lag:].copy()
        self.wy_pm      = self.PM_wy     .iloc[:, self.lag:].copy()
        
        

        self.liters_am  = self.AM_liters
        self.wy_am      = self.AM_wy
        self.liters_pm  = self.PM_liters
        self.wy_pm      = self.PM_wy

        datex2 = self.liters_am.T.index.astype(int)
        self.datex = pd.to_datetime(datex2, origin='1899-12-30', unit='D')
        self.last_index_value = self.datex[-1]

        self.maxcols     = len(self.datex)             #1575                          #len of dates (col headers for liters - which starts with 'start_date')
        self.maxrows     = len(self.bd['WY_id'])   #201                          #len of groupx - will accomodate new calves - continuous series from 1~200 will be the output col heading 

        self.idx     = np.zeros((self.maxrows+1,self.maxcols), dtype=int)    
        self.idx_am  = self.idx.copy()
        self.idx_pm  = self.idx.copy()

        # Numpyzation
        self.wy_am_np =    self.wy_am.      to_numpy(dtype=float)
        self.wy_pm_np =    self.wy_pm.      to_numpy(dtype=float)
        self.liters_am_np= self.liters_am.  to_numpy(dtype=float)
        self.liters_pm_np= self.liters_pm.  to_numpy(dtype=float)
        
        
        


    def fullday_calc(self):
        
    # AM calc

        target_am = []
        i = 0

        while i < self.maxcols:
            index1 = self.wy_am_np[:,i]    #70,1869
            index2 = np.nan_to_num(index1, nan=0).astype(int)

            value1 = self.liters_am_np[:,i]   #70 1869
            value2 = np.nan_to_num(value1, nan=0.0).astype(float)
            target1 = self.idx_am[:,i].astype(float)      #243 1869

            target1[index2] = value2
            target_am.append(target1)
            i += 1
        am1 = pd.DataFrame(target_am)

        am = am1.T
        am.columns = self.datex
        am.replace(0,np.nan,inplace=True)
        am.drop(am.columns[0], axis=1, inplace=True)

        
#   PM calc

        target_pm = []
        i = 0

        while i < self.maxcols:
            index1 = self.wy_pm_np[:,i]    #70,1869
            index2 = np.nan_to_num(index1, nan=0).astype(int)

            value1 = self.liters_pm_np[:,i]   #70 1869
            value2 = np.nan_to_num(value1, nan=0.0).astype(float)
            target1 = self.idx_pm[:,i].astype(float)      #243 1869

            target1[index2] = value2
            target_pm.append(target1)
            i += 1
        pm1 = pd.DataFrame(target_pm)


        # pm2 = pd.DataFrame(pm1)
        pm = pm1.T
        pm.columns = self.datex
        pm.replace(0,np.nan,inplace=True)
        # pm.drop(pm.iloc[:,0:1],axis=1,inplace=True)
        pm.drop(pm.columns[0], axis=1, inplace=True)

        
    # fullday calc
    
        fullday1 = np.add(am1,pm1)  #cols are wy's, index is days
        fullday2 = pd.DataFrame(fullday1)
        fullday2['datex'] = self.datex
        fullday2.set_index('datex', inplace=True)

        self.fullday = fullday2
        self.fullday.index=pd.to_datetime(self.fullday.index, errors='coerce', format="%m/%d/%Y").date
        self.fullday.replace(0,np.nan,inplace=True)

        self.fullday.drop(self.fullday.iloc[:,0:1],axis=1,inplace=True)
        self.fullday.index.name = 'datex'
        
        self.fullday_xl = self.fullday.copy()
        self.fullday_xl.index = self.fullday_xl.index.map(lambda x: (x - datetime(1899,12,30).date()).days)        
        self.fullday_lastdate = pd.DataFrame(index=[self.fullday.index[-1]], columns=['last_date'])
        

        
        return  self.fullday, self.fullday_xl, self.fullday_lastdate
        
        
 

    def ten_day(self):

        lastday = self.fullday.iloc[-1:,:]     #last milking day recorded
        ld=lastday.loc[:,(lastday>0).any()].columns.tolist()     #the .any is important
        ld_nums = [int(x-1) for x in ld] #decrements the wy's
       
        tenday1 = self.fullday.iloc[-10:,:].copy() # has all wy's
        tenday2 = tenday1.loc[:,ld]           # has milkers only
        # tenday3 = tenday1.iloc[:,ld_nums]      #unnecessary?
     
        tendayT=tenday2.T
    
        tendayT.columns=[1,2,3,4,5,6,7,8,9,10]
        tenday = tendayT
        avg = tenday.mean(axis=1).astype(float)
        tendayT['avg']=avg.round(1) 

        tenday.index.name='WY_id'
    
        # sumx = tenday.sum(axis=0).astype(float)
        # avgx = tenday.mean(axis=0).astype(float)
        # tenday.loc[''] = sumx.round(0)                   # [''] means 'empty row'
        tenday.loc['avg']   = tenday.mean(axis=0)
        tenday.loc['total'] = tenday.sum(axis=0)
        
        return tenday, tenday1
        
        

    def create_avg_count(self):

        milk = self.fullday.copy()
        self.fullday.replace(np.nan,0,inplace=True)
        
        milkrowcount =   milk.astype(bool).sum(axis=1)
        milkrowsum =     milk.sum(axis=1,skipna=True)    #sum for that day, all cows
        milkcolsum =     milk.sum(axis=0,skipna=True)    #sum for that cow for all days
        
        milk['avg'] =    milkrowsum                      #blank col sets up the group agg
        milk['count'] =  milkrowcount
        milk.index =    self.datex
        milk.index.name = 'datex'
        milk.index =    pd.to_datetime(milk.index)
        
        return milk
    
     

    def create_monthly_weekly(self):
        
        self.milk['year']   = self.milk.index.year
        self.milk['month']  = self.milk.index.month
        self.milk['week']   = self.milk.index.isocalendar().week
        #  the as_index=False leaves the new columns accessible for .loc, otherwise they become part of a multi-index
        milk_monthly_sum    =   self.milk.groupby(['year','month'],          as_index=False).sum()    
        milk_monthly_mean1  =   self.milk.groupby(['year','month'],          as_index=False).mean()
        self.weekly_sum     =   self.milk.groupby(['year','month','week'],   as_index=False).sum() 
        self.weekly_mean    =   self.milk.groupby(['year','month','week'],   as_index=False).mean()
        

        # change names because 'sum' will eventually mean the monthly total vs the avg
        milk_monthly_mean1.rename(columns={'avg':'avg sum','count':'avg count'},inplace=True)
        # cuts out the middle cols
        monthly1       = milk_monthly_mean1.iloc[-12:,[0,1,-2,-3]].copy()
    
        monthly1['total'] = milk_monthly_sum['avg']

        def format_num(num):
            return '{:,.0f}'.format(num)

        monthly1[['avg count', 'avg sum', 'total']] = monthly1[['avg count', 'avg sum', 'total']].map(format_num)
        self.monthly = monthly1.reset_index(drop=True)
        self.monthly_sum = milk_monthly_sum
        self.monthly_mean = milk_monthly_mean1

        return [self.monthly, self.monthly_sum, self.monthly_mean, 
                self.weekly_sum, self.weekly_mean] 
    
    def write_to_csv(self):
        self.fullday.to_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
        self.fullday_xl.to_csv('F:\\COWS\\data\\milk_data\\fullday_xl_format\\fullday_xl.csv')
        self.tenday.to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.csv')
        self.tenday1.to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday1.csv')


if __name__ == "__main__":
    milk_aggregates = MilkAggregates()
    milk_aggregates.write_to_csv()


    