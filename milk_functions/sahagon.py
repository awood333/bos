'''/milk_functions/sahagon.py'''

import pandas as pd
from milk_functions.milkaggregates import MilkAggregates

class sahagon:
    def __init__(self):

        # MA = MilkAggregates()
        # self.tenday = MA.tenday
        self.dm = self.get_data()
        self.dm_monthly = self.get_monthly()
        self.write_to_csv()
    
    def get_data(self):
                
        dm1 = pd.read_excel("F:\\COWS\\data\\daily_milk.ods", sheet_name='stats')
        dm2 = dm1.iloc[:4,:]

        dm_t = dm2.head(4).transpose()
        

        # Set the first row as the header and convert to datetime
        dm_t.columns = dm_t.iloc[0]
        dm_t = dm_t.drop(dm_t.index[0])
        dm_t.index = pd.to_datetime(dm_t.index, format='ISO8601')
        start_date = '2024-01-01'
        
        self.dm = dm_t.loc[start_date :,'saha total'].to_frame()
    
        
        return self.dm
    
    def get_monthly(self):
        
        year = self.dm.index.year
        month = self.dm.index.month
        
        dm_m = self.dm.groupby([year,month], group_keys=True).sum()
        
        self.dm_monthly = dm_m
        return self.dm_monthly
    
    def write_to_csv(self):
        self.dm_monthly.to_csv('F:\\COWS\\data\\PL_data\\sahagon_monthly.csv')
    
    
if __name__ == '__main__':
    sahagon = sahagon()
