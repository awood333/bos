'''wet_dry.py'''

import pandas as pd
import numpy as np

from MilkBasics import MilkBasics
from CreateStartDate import DateRange

today = pd.Timestamp.today()

class WetDry:
    def __init__(self):
        
        self.MB         = MilkBasics()
        self.DR         = DateRange()
        
        self.ext_rng    = self.MB.data['ext_rng'] # start is 9-1/2016, end is last milking day
        self.milk1      = self.MB.data['milk'] # this is 'milk' betw the cutoff dates/WY's
        self.datex      = self.MB.datex #same index as fullday
        self.WY_ids  = self.MB.data['stop'].columns  # self.WY_ids integer
                
        self.wet_days_df1, self.wet_sum_df1, self.wet_max_df1  = self.create_wet_days()
        self.wdd, self.wsd, self.wmd =  self.reindex_columns()
        self.wdd_monthly = self.create_monthly_wet_days()
        self.write_to_csv()

    def create_wet_days(self):
        wet_days1       = pd.DataFrame()
        wet_days2       = pd.DataFrame()
        self.wet_days3  = pd.DataFrame()
                
        wet_sum1, wet_sum2, self.wet_sum3= [],[],[]
        wet_max1, wet_max2, self.wet_max3 = [],[], []
        
        idx     = self.ext_rng
        lacts   = self.MB.data['stop'].index      # lact# float

        for i in self.WY_ids:
            wet_days2 = pd.DataFrame()
            for j in lacts:

                lastday = self.MB.data['lastday']  # last day of the milk df datex

                start = self.MB.data ['start'].loc[j, i]
                stop  = self.MB.data ['stop'] .loc[j, i]

                a = pd.isna(start)  is False  # start value exists
                b = pd.isna(stop)   is False  # stop value exists
                e = pd.isna(start)  is True   # start value missing
                f = pd.isna(stop)   is True   # stop value missing

                # completed lactation:
                if a and b:
                    days_range = pd.date_range(start, stop)
                    day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                    wet_days1 = pd.DataFrame(day_nums, days_range)
                    
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    wet_sum1 = wet1a.sum()
                    wet_max1 = wet1a.max()
                    

                elif a and f:
                    days_range = pd.date_range(start, lastday)
                    day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                    wet_days1 = pd.DataFrame(day_nums, days_range)
                    
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    wet_sum1 = wet1a.sum()
                    wet_max1 = wet1a.max()

                    
                else:
                    pass
                
                wet_days2 = pd.concat ([wet_days2, wet_days1],axis=0)
                wet_sum2 .append (wet_sum1)
                wet_max2 .append (wet_max1)
                
                wet_days1=pd.DataFrame()

            wet_days2a = wet_days2.reindex(idx)
            wet_days2b = wet_days2a.rename(columns = {0: i})
            
            if not wet_days2b.empty:
                self.wet_days3 = pd.concat ([self.wet_days3, wet_days2b], axis=1) 
                self.wet_sum3 .append (wet_sum2)
                self.wet_max3 .append (wet_max2)
                
        if not self.wet_days3.empty:            
            self.wet_days_df1    = pd.DataFrame(self.wet_days3)
            self.wet_sum_df1    = pd.DataFrame(self.wet_sum3) 
            self.wet_max_df1       = pd.DataFrame(self.wet_max3)
       
        return self.wet_days_df1, self.wet_sum_df1, self.wet_max_df1 
    
    
    def reindex_columns(self):
        cols = self.MB.data['rng']
        wddt1 = self.wet_days_df1.T
        wddt2 = wddt1.reindex(cols)
        self.wdd = wddt2.T
        
        wsdt1 = self.wet_sum_df1.T
        wsdt2 = wsdt1.reindex(cols)
        self.wsd = wsdt2.T
        
        wmdt1 = self.wet_max_df1.T
        wmdt2 = wmdt1.reindex(cols)
        self.wmd = wmdt2.T
        
        return self.wdd, self.wsd, self.wmd
    
    
    def create_monthly_wet_days(self):
        
        wdd_m = self.wdd.copy()
        wdsum_m = self.wsd.copy()
        wdmax_m = self.wmd.copy()

        
        groupA_count = (wdd_m <= 210).sum(axis=1)
        groupB_count = (wdd_m > 210) .sum(axis=1)
        
        
        wdd_m['groupA_count'] = groupA_count
        wdd_m['groupB_count'] = groupB_count

        year = wdd_m.index.year
        month = wdd_m.index.month
        days = wdd_m.index.days_in_month
        
        wdd_m.index = pd.MultiIndex.from_arrays(
            [ year, month, days], names=['year','month', 'days' ]
            )
        
        wdd_monthly1 = wdd_m.iloc[:,-2:].copy()
        
        self.wdd_monthly = wdd_monthly1.groupby(['year','month', 'days']).mean()
        
        self.wdd_monthly = self.wdd_monthly.loc[2024:,:]
        
        return self.wdd_monthly
    
    def write_to_csv(self):
        
        self.wdd.to_csv('F:\\COWS\\data\\wet_dry\\wdd.csv')       
        self.wsd.to_csv('F:\\COWS\\data\\wet_dry\\wdsum.csv')
        self.wmd.to_csv('F:\\COWS\\data\\wet_dry\\wdmax.csv')
        self.wdd_monthly.to_csv('F:\\COWS\\data\\wet_dry\\wdd_monthly.csv')
        



if __name__ == '__main__':
    WetDry()