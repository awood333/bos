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
        self.WY_ids     = self.MB.data['start'].columns  # self.WY_ids integer

                
        [self.wet_days_df1,  self.wsd, 
         self.wmd, self.milking_liters3]  = self.create_wet_days()
        self.wdd                                =  self.reindex_columns()
        self.wdd_monthly                        = self.create_monthly_wet_days_by_group()
        self.write_to_csv()

    def create_wet_days(self):
        wet_days1   = wet_days2 = wet_days3     = pd.DataFrame()         
        wet_sum_df1 = wet_sum2  = wet_sum3      = pd.DataFrame()
        wet_max_df1    = wet_max2  = wet_max3      = pd.DataFrame()
        milking_liters1 = milking_liters2 = self.milking_liters3       = pd.DataFrame()
        
        idx     = self.ext_rng
        WY_ids  = self.MB.data['WY_ids']   #NOTE:put a 1 to slice
        # WY_ids = WY_ids1[250:254]
        i=0
        
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
                    wet_days1 = pd.DataFrame(day_nums, days_range)
                
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    wet_sum1 = pd.DataFrame([wet1a.sum()], columns=[j], index=[i])
                    wet_max1 = pd.DataFrame([wet1a.max()], columns=[j], index=[i])
                    
                
                # ongoing lactation
                elif a and f:
                    days_range = pd.date_range(start, lastday)
                    day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                    wet_days1 = pd.DataFrame(day_nums, days_range)
                    
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    milking_liters1 = pd.Series(wet1a)
                    
                    wet_sum1 = pd.DataFrame([wet1a.sum()], columns=[j], index=[i])
                    wet_max1 = pd.DataFrame([wet1a.max()], columns=[j], index=[i])

                # first calf - still milking
                elif e and f:
                    wet1a    = pd.DataFrame(columns=[j])
                    wet_sum1 = pd.DataFrame(columns=[j], index=[i])
                    wet_max1 = pd.DataFrame(columns=[j], index=[i])
                    
                                
                wet_days2 = pd.concat([wet_days2, wet_days1],axis=0)
                wet_sum2  = pd.concat([wet_sum2, wet_sum1], axis=1 )
                wet_max2  = pd.concat([wet_max2, wet_max1], axis=1 )
                
                milking_liters1 = milking_liters1.reset_index(drop=True)
                milking_liters2 = pd.concat([milking_liters2, milking_liters1], axis=1)

                
                wet_days1 = pd.DataFrame()
                wet_sum1 = wet_max1 = pd.DataFrame()
                milking_liters1 = pd.DataFrame()


            wet_days2a = wet_days2  .reindex(idx)
            wet_days2b = wet_days2a .rename(columns = {0: i})

            
            
            # Fill NaN values with 0 before concatenation
            wet_days2b  = wet_days2b.astype(float)  .fillna(0)
            wet_sum2    = wet_sum2  .astype(float)  .fillna(0)
            wet_max2    = wet_max2  .astype(float)  .fillna(0)
                        
            if not wet_days2b.empty:
                wet_days3 = pd.concat( [wet_days3, wet_days2b],  axis=1) 
                wet_sum3 = pd.concat(  [wet_sum3, wet_sum2],    axis=0)
                wet_max3 = pd.concat(  [wet_max3, wet_max2],    axis=0)
            # print(f"wet_max3 after appending wet_max2 for WY_id {i}: {wet_max3}")
                
            wet_days2 = pd.DataFrame()
            wet_sum2 = pd.DataFrame()
            wet_max2 = pd.DataFrame()
                
        if not wet_days3.empty:            
            self.wet_days_df1   = pd.DataFrame(wet_days3)
            wet_sum_df1         = pd.DataFrame(wet_sum3) 
            wet_max_df1         = pd.DataFrame(wet_max3)
            
        wsd1 = wet_sum_df1
        wsd2 = wsd1.reindex(self.MB.data['WY_ids'])
        self.wsd = wsd2
        
        wmd1 = wet_max_df1
        wmd2 = wmd1.reindex(self.MB.data['WY_ids'])
        self.wmd = wmd2
        
        self.milking_liters3 = milking_liters2
            
        return self.wet_days_df1, self.wsd, self.wmd , self.milking_liters3
    
    
    def reindex_columns(self):
        cols    = self.MB.data['WY_ids']
        wddt1   = self.wet_days_df1.T
        wddt2   = wddt1.reindex(cols)
        self.wdd = wddt2.T
        return self.wdd
    
    
    def create_monthly_wet_days_by_group(self):
        
        wdd_m = self.wdd.copy()
        
        groupA_count = (wdd_m <= 210).sum(axis=1)  #210 days = 30 weeks
        groupB_count = (wdd_m > 210) .sum(axis=1)
        
        
        wdd_m['groupA_count'] = groupA_count
        wdd_m['groupB_count'] = groupB_count

        year = wdd_m.index.year
        month = wdd_m.index.month
        days = wdd_m.index.days_in_month
        
        wdd_m.index = pd.MultiIndex.from_arrays(
            [ year, month], names=['year','month' ]
            )
        
        wdd_monthly1 = wdd_m.iloc[:,-2:].copy()
        
        self.wdd_monthly = wdd_monthly1.groupby(['year','month']).mean()
        
        self.wdd_monthly = self.wdd_monthly.loc[2024:,:]
        
        return self.wdd_monthly
    
    def write_to_csv(self):


        self.wdd.iloc[-25:,:].to_csv('F:\\COWS\\data\\wet_dry\\wd_days.csv')               
        self.wdd             .to_csv('F:\\COWS\\data\\wet_dry\\full_wdd.csv')       
        self.wsd             .to_csv('F:\\COWS\\data\\wet_dry\\wd_sum.csv')
        self.wmd             .to_csv('F:\\COWS\\data\\wet_dry\\wd_max.csv')
        self.wdd_monthly     .to_csv('F:\\COWS\\data\\wet_dry\\wd_days_monthly.csv')
        



if __name__ == '__main__':
    WetDry()