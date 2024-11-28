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
        self.datex      = self.MB.datex
                
        self.wet_days_df, self.wet_sum_df, self.wet_max_df  = self.create_wet_days()
        self.wdd_monthly = self.create_monthly_wet_days()

    def create_wet_days(self):
        wet_days1       = pd.DataFrame()
        wet_days2       = pd.DataFrame()
        self.wet_days3  = pd.DataFrame()
                
        wet_sum1, wet_sum2, self.wet_sum3= [],[],[]
        wet_max1, wet_max2, self.wet_max3 = [],[], []

        WY_ids  = self.MB.data['stop'].columns  # WY_ids integer
        lacts   = self.MB.data['stop'].index      # lact# float

        for i in WY_ids:
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
                    wet_days1 = pd.DataFrame(day_nums)
                    
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    wet_sum1 = wet1a.sum()
                    wet_max1 = wet1a.max()


                elif a and f:
                    days_range = pd.date_range(start, lastday)
                    day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                    wet_days1 = pd.DataFrame(day_nums)
                    
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    wet_sum1 = wet1a.sum()
                    wet_max1 = wet1a.max()
                    
                    
                elif e and f:
                    wet_days1=pd.DataFrame()
                    wet1a, wet_sum1, wet_max1=[],[],[]
                    
                else:
                    pass


                wet_days2 = pd.concat ([wet_days2, wet_days1],axis=0)
                # duplicate_labels = wet_days2.index[wet_days2.index.duplicated()]
                # print('dup labels  ',i,j,  duplicate_labels)
                wet_sum2 .append (wet_sum1)
                wet_max2 .append (wet_max1)

            wet_days2 = wet_days2.reindex(self.DR.date_range_daily)
            # duplicate_labels = wet_days2.index[wet_days2.index.duplicated()]
            # print('wet 2  dup labels  ',i,j, duplicate_labels)
                
            self.wet_days3 = pd.concat ([self.wet_days3, wet_days2], axis=1) 
            wet_days2=pd.DataFrame()
            self.wet_sum3 .append (wet_sum2)
            self.wet_max3 .append (wet_max2)            
            
        self.wet_days_df    = pd.DataFrame(self.wet_days3)
        self.wet_sum_df    = pd.DataFrame(self.wet_sum3) 
        self.wet_max_df       = pd.DataFrame(self.wet_max3)
        
        
        return self.wet_days_df, self.wet_sum_df, self.wet_max_df 
    
    def create_monthly_wet_days(self):
        
        wdd = self.wet_days_df
        wdsum = self.wet_sum_df
        wdmax = self.wet_max_df

        
        groupA_count = (wdd <= 210).sum(axis=1)
        groupB_count = (wdd > 210) .sum(axis=1)
        
        
        wdd['groupA_count'] = groupA_count
        wdd['groupB_count'] = groupB_count

        year = wdd.index.year
        month = wdd.index.month
        days = wdd.index.days_in_month
        
        wdd.index = pd.MultiIndex.from_arrays([ year, month, days], \
            names=['year','month', 'days' ])
        
        wdd_monthly1 = wdd.iloc[:,-2:].copy()
        
        self.wdd_monthly = wdd_monthly1.groupby(['year','month', 'days']).mean()
        
        self.wdd_monthly = self.wdd_monthly.loc[2024:,:]
        
        return self.wdd_monthly
    
    def write_to_csv(self):
        
        self.wet_days_df.to_csv('F:\\COWS\\data\\wet_dry\\wdd.csv')       
        self.wet_sum_df.to_csv('F:\\COWS\\data\\wet_dry\\wdsum.csv')
        self.wet_max_df.to_csv('F:\\COWS\\data\\wet_dry\\wdmax.csv')
        self.wdd_monthly.to_csv('F:\\COWS\\data\\wet_dry\\wdd_monthly.csv')
        



if __name__ == '__main__':
    WetDry()