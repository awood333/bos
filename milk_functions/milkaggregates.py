
'''milk_related\\milk_aggregates.py'''

from datetime import datetime
import sys
import os
import pandas as pd
import numpy as np

from MilkBasics import MilkBasics
from insem_functions.Insem_ultra_data import InsemUltraData
# from milk_functions.sahagon import sahagon


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

IUD = InsemUltraData()

class MilkAggregates:
    def __init__(self):
        
        self.data = MilkBasics().data
        self.allx = IUD.allx
        # self.sahagon = sahagon()

        self.lag     = -10
        print('lag = ', self.lag)
        
        self.date_format='%m/%d/%Y'
        
        [self.LBP, self.RBP]                    = self.create_basepath()
        
        [self.maxcols, self.idx_am, self.idx_pm, 
        self.wy_am_np, self.wy_pm_np,
        self.liters_am_np, self.liters_pm_np ]   = self.basics()
        
        [ self.am,      self.pm,
        self.fullday,   self.fullday_xl,
        self.fullday_lastdate]                  = self.fullday_calc()
        
        self.halfday_AM, self.halfday_PM, self.halfday        = self.halfday_AM_PM()
        
        self.tenday, self.tenday1               = self.ten_day()
        
        [self.monthly, self.weekly,
         self.start, self.stop]                 = self.create_monthly_weekly()

        self.write_to_csv()
 

    def create_basepath(self):

        if os.name == 'nt':  # Windows
            self.LBP = 'F:\\COWS\\data\\milk_data\\'
            self.RBP = 'Z:/My Drive/COWS/data/milk_data'
        elif os.name == 'posix':  # Linux
            self.LBP = '/home/alanw/data/milk_data'
            self.RBP = 'gdrive:My Drive/COWS/data/milk_data'

        return self.LBP, self.RBP
    

    def basics(self):       

        self.AM_liters = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',index_col=0, header=0)
        self.AM_wy     = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',index_col=0, header=0)
        self.PM_liters = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',index_col=0, header=0)
        self.PM_wy     = pd.read_csv('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',index_col=0, header=0)
        
        self.daily_milk = pd.read_excel("F:\\COWS\\data\\daily_milk.ods")
       
        wy      = self.data['bd']['WY_id']
        alive1  = self.data['bd']['death_date'].isnull()
        alive   = wy.loc[alive1].copy()
        alive.reset_index(drop=True,inplace=True)

        liters_am  = self.AM_liters .iloc[:, self.lag:].copy()
        wy_am      = self.AM_wy     .iloc[:, self.lag:].copy()
        liters_pm  = self.PM_liters .iloc[:, self.lag:].copy()
        wy_pm      = self.PM_wy     .iloc[:, self.lag:].copy()

        liters_am  = self.AM_liters
        wy_am      = self.AM_wy
        liters_pm  = self.PM_liters
        wy_pm      = self.PM_wy

        datex2 = liters_am.T.index.astype(int)
        self.datex = pd.to_datetime(datex2, origin='1899-12-30', unit='D')
        last_index_value = self.datex[-1]
        print('last index value ', last_index_value)

        self.maxcols     = len(self.datex)             #1575                          #len of dates (col headers for liters - which starts with 'start_date')
        maxrows     = len(self.data['bd']['WY_id'])   #201                          #len of groupx - will accomodate new calves - continuous series from 1~200 will be the output col heading 

        idx     = np.zeros((maxrows+1,self.maxcols), dtype=int)    
        self.idx_am  = idx.copy()
        self.idx_pm  = idx.copy()

        # Numpyzation
        self.wy_am_np =    wy_am.      to_numpy(dtype=float)
        self.wy_pm_np =    wy_pm.      to_numpy(dtype=float)
        self.liters_am_np= liters_am.  to_numpy(dtype=float)
        self.liters_pm_np= liters_pm.  to_numpy(dtype=float)
        
        return [self.maxcols, self.idx_am, self.idx_pm, 
                self.wy_am_np, self.wy_pm_np,
                self.liters_am_np, self.liters_pm_np   ]


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

        self.am = am1.T
        self.am.columns = self.datex
        self.am.replace(0,np.nan,inplace=True)
        self.am.drop(self.am.columns[0], axis=1, inplace=True)

        
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
        self.pm = pm1.T
        self.pm.columns = self.datex
        self.pm.replace(0,np.nan,inplace=True)
        # self.pm.drop(self.pm.iloc[:,0:1],axis=1,inplace=True)
        self.pm.drop(self.pm.columns[0], axis=1, inplace=True)
        

        
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
        
        return  [self.am, self.pm,
            self.fullday, self.fullday_xl, 
            self.fullday_lastdate]
        
        
    def halfday_AM_PM(self):
        lastday_AM = self.am.iloc[:,-1:]
        ldam=lastday_AM.loc[(lastday_AM.notna() ).any(axis=1),:].index.tolist()  
        
        lastday_PM = self.pm.iloc[:,-1:]
        ldpm=lastday_PM.loc[(lastday_PM.notna() ).any(axis=1),:].index.tolist() 
        
        self.halfday_AM = lastday_AM.loc[ldam,:]
        self.halfday_PM = lastday_PM.loc[ldpm,:]    
        
        self.halfday = self.halfday_AM.merge(self.halfday_PM, how='left', left_index=True, right_index=True)
        self.halfday.columns = ['AM', 'PM']
        self.halfday.index.name = str(pd.to_datetime(self.datex[-1]).strftime('%Y-%m-%d'))
        
            
        return self.halfday_AM, self.halfday_PM, self.halfday


    def ten_day(self):

        lastday = self.fullday.iloc[-1:,:]     #last milking day recorded
        ld=lastday.loc[:,(lastday>0).any()].columns.tolist()     #the .any is important
        ld_nums = [int(x-1) for x in ld] #decrements the wy's
       
        self.tenday1 = self.fullday.iloc[-10:,:].copy() # has all wy's
        tenday2 = self.tenday1.loc[:,ld]           # has milkers only
        
        tenday_cols1 = self.tenday1.index.to_list()
        tenday_cols  = [date.strftime('%m-%d') for date in tenday_cols1]     
     
        tendayT=tenday2.T
    
        tendayT.columns=tenday_cols
  
        avg = tendayT.mean(axis=1).astype(float)
        tendayT['avg']=avg.round(1)

        
        tendayT.index.name='WY_id'
    
        tendayT.loc['avg']   = tendayT.mean(axis=0)
        tendayT.loc['total'] = tendayT.iloc[:-1,:].sum(axis=0)
                
        lastcol = tendayT.iloc[:,9]
        tendayT['pct chg from avg'] = ((lastcol/ tendayT['avg'] ) - 1)
        
        days1 = pd.DataFrame(self.allx.loc[:,['WY_id','days milking', 'u_read', 'expected bdate']])
        days = days1.set_index('WY_id')
        days.index = days.index.astype('int').astype('str')
        tendayT.index = tendayT.index.astype('str')
 
        self.tenday = tendayT.merge(days, 
                    how='left', 
                    left_index=True, 
                    right_index=True
                                    )
        

        
        return self.tenday, self.tenday1
    


    def create_monthly_weekly(self):
        
        def format_num(num):
            return '{:,.0f}'.format(num)

        milk1 = self.fullday.copy()
        milk1.index = pd.to_datetime(milk1.index)
        
        start1 = '9/1/2023'
        self.start = pd.to_datetime(start1)
        self.stop = milk1.index[-1]
        
        milk = milk1.loc[self.start:,:].copy()
  
        milkrowsum =     milk.sum(axis=1,skipna=True)    #sum for that day, all cows
        milkrowcount = milk.count(axis=1)               # count of cows on that day
        
        milk['sum'] =    milkrowsum                      #blank col sets up the group agg
        milk['count'] =  milkrowcount
        milk['year']   =   milk.index.year
        milk['month']  =   milk.index.month
        
        self.monthly    =   milk.groupby(['year','month'],as_index=False).agg({'sum': 'sum', 'count':'mean'})

        # this placement of 'milk' adds the week col and keeps the col arrangement straight in monthly
        milk['week']   =   milk.index.isocalendar().week

        self.weekly     =   milk.groupby(['year','month','week'],   as_index=False).agg({'sum': 'sum', 'count':'mean'})

        self.monthly[['count', 'sum']] = self.monthly[['count', 'sum']].map(format_num)
        self.weekly [['count', 'sum']] = self.weekly [['count', 'sum']].map(format_num)

        return self.monthly,  self.weekly, self.start, self.stop
    
    def write_to_csv(self):
        self.fullday    .to_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
        self.fullday_xl .to_csv('F:\\COWS\\data\\milk_data\\fullday_xl_format\\fullday_xl.csv')
        self.tenday     .to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.csv')
        self.tenday1    .to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday1.csv')
        self.monthly    .to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\monthly.csv')
        self.weekly     .to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\weekly.csv')
        self.halfday    .to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\halfday.csv')
        
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\fullday\\fullday.xlsx', engine='openpyxl') as writer:
        #     self.fullday.to_excel(writer, sheet_name='fullday')
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\fullday_xl_format\\fullday_xl.xlsx', engine='openpyxl') as writer:
        #     self.fullday_xl.to_excel(writer, sheet_name='fullday_xl')
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.xlsx', engine='openpyxl') as writer:
        #     self.tenday.to_excel(writer, sheet_name='tenday')
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday1.xlsx', engine='openpyxl') as writer:
        #     self.tenday1.to_excel(writer, sheet_name='tenday1')
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\monthly.xlsx', engine='openpyxl') as writer:
        #     self.monthly.to_excel(writer, sheet_name='monthly')
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\weekly.xlsx', engine='openpyxl') as writer:
        #     self.weekly.to_excel(writer, sheet_name='weekly')
        # with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\halfday.xlsx', engine='openpyxl') as writer:
        #     self.halfday.to_excel(writer, sheet_name='halfday')
if __name__ == '__main__':
    milk_aggregates = MilkAggregates()
    