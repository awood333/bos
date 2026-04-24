'''milk_functions\\milk_aggregates.py

Second half of milk aggregation: halfday, tenday, monthly/weekly summaries.
Depends on milk_aggregates_basic (which provides fresh fullday and AM/PM matrices)
plus insem_ultra_data (for days-milking merge).
'''

import sys
import os
import inspect
import pandas as pd
# import numpy as np

from container import get_dependency

# from utilities.logging_setup import  setup_debug_logging, debug_method
# import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class MilkAggregates:

    print(f"MilkAggregates instantiated by: {inspect.stack()[1].filename}")

    def __init__(self):
        self.MAB = None
        self.MB = None
        self.data = None
        self.DR = None
        self.IUB = None
        self.IUD = None
        self.allx = None
        self.am = None
        self.pm = None
        self.fullday = None
        self.fullday_lastdate = None
        self.datex = None
        self.AM_liters = None
        self.halfday_AM = None
        self.halfday_PM = None
        self.halfday = None
        self.tenday = None
        self.tenday1 = None
        self.monthly_summary = None
        self.weekly_summary = None
        self.monthly_avg = None
        self.weekly_avg = None
        self.start = None
        self.stop = None

    def load_and_process(self):
        self.MAB  = get_dependency('milk_aggregates_basic')
        self.MB   = self.MAB.MB
        self.data = self.MB.data
        self.DR   = get_dependency('date_range')
        self.IUB  = get_dependency('insem_ultra_basics')
        self.IUD  = get_dependency('insem_ultra_data')
        self.allx = self.IUD.allx

        # Pull computed results from MAB
        self.am               = self.MAB.am
        self.pm               = self.MAB.pm
        self.fullday          = self.MAB.fullday
        self.fullday_lastdate = self.MAB.fullday_lastdate
        self.datex            = self.MAB.datex
        self.AM_liters        = self.MAB.AM_liters

        self.halfday_AM, self.halfday_PM, self.halfday = self.halfday_AM_PM()

        self.tenday, self.tenday1 = self.ten_day()

        [self.monthly_summary, self.weekly_summary,
         self.start, self.stop,
         self.monthly_avg, self.weekly_avg] = self.create_monthly_weekly()

        self.write_to_csv()
        
        
    def halfday_AM_PM(self):
        lastday_AM = self.am.iloc[:,-1:]
        ldam=lastday_AM.loc[(lastday_AM.notna() ).any(axis=1),:].index.tolist()  
        
        lastday_PM = self.pm.iloc[:,-1:]
        ldpm=lastday_PM.loc[(lastday_PM.notna() ).any(axis=1),:].index.tolist() 
        
        self.halfday_AM = lastday_AM.loc[ldam,:]
        self.halfday_PM = lastday_PM.loc[ldpm,:]    
        
        self.halfday = self.halfday_AM.merge(self.halfday_PM, how='left', left_index=True, right_index=True)
        self.halfday.columns = ['AM', 'PM']
        self.halfday.index.name = 'WY_id'
        self.halfday = self.halfday.reset_index()
            
        return self.halfday_AM, self.halfday_PM, self.halfday


    def ten_day(self):

        lastday = self.fullday.iloc[-1:,:]     #last milking day recorded
        ld=lastday.loc[:,(lastday>0).any()].columns.tolist()     #the .any is important
        # ld_nums = [int(x-1) for x in ld] #decrements the wy's
       
        self.tenday1 = self.fullday.iloc[-10:,:].copy() # has all wy's
        tenday2 = self.tenday1.loc[:,ld]           # has milkers only
        
        tenday_cols1 = self.tenday1.index.to_list()
        tenday_cols  = [date.strftime('%m-%d') for date in tenday_cols1]     
     
        tendayT=tenday2.T
    
        tendayT.columns=tenday_cols
        avg = tendayT.mean(axis=1)
        tendayT['avg'] = avg
        lastcol = tendayT.iloc[:,9]
        tendayT['pct chg from avg'] = ((lastcol/ tendayT['avg'] ) - 1)
           
        tendayT.loc['total'] = tendayT.iloc[:-1,:].sum(axis=0)

        # Round the first 11 columns to 1 decimal place
        for col in tendayT.columns[:11]:
            tendayT[col] = tendayT[col].round(1).astype(str)
  

        tendayT['avg']=avg.round(1).astype(str)

        
        tendayT.index.name='WY_id'
 
                
   
        
        days1 = pd.DataFrame(self.allx.loc[:,['WY_id','days milking', 'u_read', 'expected bdate']])
        days = days1.set_index('WY_id')
        days.index = days.index.astype('int').astype('str')
        tendayT.index = tendayT.index.astype('str')
 
        tenday2 = tendayT.merge(days, 
                    how='left', 
                    left_index=True, 
                    right_index=True
                                    )
        # tenday2.index.name = 'WY_id_1'
        tenday3 = tenday2.reset_index()
        self.tenday = tenday3
        

        
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
        
        milk['sum']     = milkrowsum                      #blank col sets up the group agg
        milk['count']   = milkrowcount
        milk['year']    = milk.index.year
        milk['month']   = milk.index.month
        milk['week']    = milk.index.isocalendar().week
        
        self.monthly_summary=   milk.groupby(['year','month'],as_index=False).agg({'sum': 'sum', 'count':'mean'})
        self.monthly_avg    =   milk.groupby(['year','month'],as_index=False).agg('mean')

        self.weekly_summary =   milk.groupby(['year','month', 'week'],as_index=False).agg({'sum': 'sum', 'count':'mean'})
        self.weekly_avg     = milk.groupby(['year','month', 'week'],as_index=False).agg('mean')

        self.monthly_summary[['count', 'sum']] = self.monthly_summary[['count', 'sum']].map(format_num)
        self.weekly_summary [['count', 'sum']] = self.weekly_summary [['count', 'sum']].map(format_num)

        return [self.monthly_summary, self.weekly_summary, 
            self.start, self.stop,
            self.monthly_avg, self.weekly_avg]
    

    def write_to_csv(self):
        print(">>> write_to_csv called")
        self.fullday         .to_csv(r"Q:\\My Drive\\COWS\\milk_data\\fullday\\fullday.csv")
        self.tenday1         .to_csv(r"Q:\\My Drive\\COWS\\milk_data\\totals\\milk_aggregates\\tenday1.csv")

        self.monthly_summary .to_csv(r"Q:\My Drive\COWS\milk_data\totals\milk_aggregates\\monthly_summary.csv")
        self.weekly_summary  .to_csv(r"Q:\My Drive\COWS\milk_data\totals\milk_aggregates\\weekly_summary.csv")
        self.monthly_avg     .to_csv(r"Q:\My Drive\COWS\milk_data\totals\milk_aggregates\\monthly_avg.csv")
        self.weekly_avg      .to_csv(r"Q:\My Drive\COWS\milk_data\totals\milk_aggregates\\weekly_avg.csv")

        self.halfday         .to_csv(r"Q:\\My Drive\\COWS\\milk_data\\totals\\milk_aggregates\\halfday.csv")
        
      
if __name__ == '__main__':
    obj=MilkAggregates()
    obj.load_and_process()      