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
                
        self.wet_days_data = self.create_wet_days()
        self.wet_dry_days_monthly = self.create_monthly_wet_days()

    def create_wet_days(self):
        wet_days2       = pd.DataFrame()
        self.wet_days3  = pd.DataFrame()
                
        wet_sum2, wet_max2 = [],[]
        self.wet_sum3, self.wet_max3 = [],[]

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
            
        self.wet_days_data = [self.wet_days3, self.wet_sum3, self.wet_max3]
        return self.wet_days_data
    
    def create_monthly_wet_days(self):
        
        wdd = self.wet_days_data
        year = wdd.index.year
        month = wdd.index.month
        
        wdd.index = pd.MultiIndex.from_arrays([ year, month, wdd.index], \
            names=['year','month','datex'])
        self.wet_dry_days_monthly = wdd.loc[2024]
        
        return self.wet_dry_days_monthly
        



if __name__ == '__main__':
    WetDry()