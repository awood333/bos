'''/milk_functions/sahagon.py'''
from datetime import datetime
import pandas as pd
from container import get_dependency

class sahagon:
    def __init__(self):
        self.DR = get_dependency('date_range')
        self.today = datetime.now()
        self.start_date = pd.to_datetime(self.DR.start_date()).normalize() 
        self.dm_daily = None
        self.dm_monthly = None

    def load_and_process(self):
        self.dm_daily = self.get_data()
        self.dm_monthly = self.get_monthly()
        self.write_to_csv()
    
    def get_data(self):
                
        dm1 = pd.read_excel("F:\\COWS\\data\\daily_milk.ods", sheet_name='stats')
        dm2 = dm1.iloc[1,:].copy()
        dm2.index   = pd.to_datetime( dm2.index, format='ISO8601', errors='coerce')

        dm3 = pd.DataFrame(dm2)
        dm4 = dm3.iloc[1:,:].copy()
        dm5 = dm4.rename(columns={ 2 : 'liters'})
        
        # Set index to date only for slicing
        dm5.index = pd.to_datetime(dm5.index).normalize()  #normalize removes the time portion
        # Use boolean indexing to avoid KeyError if start_date not in index
        dm6 = dm5[dm5.index >= self.start_date].copy()
                
        self.dm_daily = dm6
        
        return self.dm_daily
    
    def get_monthly(self):
        # Add year and month as columns for grouping
        df = self.dm_daily.copy()
        df['year'] = df.index.year
        df['month'] = df.index.month

        dm_m = df.groupby(['year', 'month'], group_keys=True).sum(numeric_only=True)
        self.dm_monthly = dm_m
        return self.dm_monthly
    
    def write_to_csv(self):
        today_str = self.today.strftime('%Y-%m-%d')
        self.dm_daily         .to_csv(f'F:\\COWS\\data\\milk_data\\totals\\sahagon\\sahagon_daily_{today_str}.csv')
        self.dm_monthly.to_csv('F:\\COWS\\data\\PL_data\\sahagon_monthly.csv')
    
    
if __name__ == '__main__':
    obj = sahagon()
    obj.load_and_process()   
