'''milk_functions\\days_of_milking.py'''

import pandas as pd
import numpy as np
# from datetime import datetime, timedelta
from milk_functions.lactation_basics import Lactation_basics
from milk_functions.WetDryBasics import WetDryBasics
from feed_related.CreateStartDate import DateRange

    
class DaysOfMilking:
    def __init__(self):
        self.L_basics   = Lactation_basics()
        self.WDB        = WetDryBasics()
        self.DOM        = self.L_basics.day_of_milking4
        
        date_range              = DateRange()
        self.startdate          = date_range.startdate
        self.enddate_monthly    = date_range.enddate_monthly
        self.enddate_daily      = date_range.enddate_daily
        
        self.date_range_daily   = date_range.create_date_range_daily()
        self.date_range_monthly = date_range.create_date_range_monthly()

        
        self.processDaysOfMilking()
        # self.result_df2             = self.fix_col_headers()
        # self.countA, self.countB    = self.create_group_dfs()
        
        # [self.countA_monthly, 
        #     self.countB_monthly, 
        #     self.all_monthly  ]  = self.create_monthly()
        
        # self.days_of_milking_dash_vars  = self.get_dash_vars()
        # self.write_to_csv()


    def processDaysOfMilking(self):
        date_range = pd.date_range(start='2016-09-01', end=self.enddate_daily, freq='D')
        rng = pd.date_range(start='2016-09-01', end='2016-09-02')
        series2 = pd.DataFrame(index=rng)
        series3 = pd.DataFrame(index=rng)
        series4 = pd.DataFrame(index=rng)
        series5=[]  
        
        dom = self.DOM
        
        days1 = dom[0]
        # print(days1)
        
        
        return














    # def fix_col_headers(self):
    #     df1 = self.result_df
    #     dfcols = self.result_df.columns
    #     newheaders=[]
    #     for i in dfcols:
    #         header =  i.split('_',1)[1]
    #         newheaders.append(header)
            
    #     df1.columns = newheaders
    #     self.result_df2 = df1
        
    #     return self.result_df2
    
    # def create_group_dfs(self):
        
    #     x=self.result_df2.map(lambda x: (x <= 150))
    #     y=self.result_df2.map(lambda x: (x >  150))
    #     self.countA = x.sum(axis=1)
    #     self.countB = y.sum(axis=1)
        
    #     return self.countA, self.countB
        
    # def create_monthly(self):

    #     year                = self.countA.index.year
    #     month               = self.countB.index.month
    #     self.countA_monthly = self.countA.groupby([year, month]).sum()
    #     self.countB_monthly = self.countB.groupby([year, month]).sum()
        
    #     self.all_monthly    = self.df_results.groupby([year, month]).sum()
        
    #     return self.countA_monthly, self.countB_monthly, self.all_monthly



    
    # def get_dash_vars(self):
    #     self.days_of_milking_dash_vars = {name: value for name, value in vars(self).items()
    #            if isinstance(value, (pd.DataFrame, pd.Series))}
    #     return self.days_of_milking_dash_vars  
    
    # def write_to_csv(self):
    #     self.result_df.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daysOfMilking_daily.csv')
    #     self.all_monthly.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daysOfMilking_monthly.csv')
    #     self.countA_monthly.to_csv('F:\\COWS\\data\\milk_data\\lactations\\countA_monthly.csv')
    #     self.countB_monthly.to_csv('F:\\COWS\\data\\milk_data\\lactations\\countB_monthly.csv')



if __name__ == "__main__":
    days = DaysOfMilking()
