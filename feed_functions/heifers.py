'''heifers.py'''

import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from feed_functions.feed_cost_basics import FeedCostBasics
from insem_functions.Insem_ultra_data import InsemUltraData

class Heifers:
    def __init__(self):

        self.FCB = FeedCostBasics()
        self.IUD = InsemUltraData()
        self.today = datetime.now()
        self.rng = pd.date_range('2023-01-01', self.today)
 
        
        self.heifers, self.WY_ids,        = self.create_heifer_df()
        self.days3, self.date_col       = self.create_heifer_days()
        
        self.milk_drinking_days = self.calc_milkdrinking_days()
        self.TMR_amt_days = self.calc_TMR_days()
        self.yellow_beans_amt_days = self.create_yellow_beans_days()
        
        
    def create_heifer_df(self):
        today = datetime.now()
        heifers1 = pd.read_csv("F:\\COWS\\data\\csv_files\\heifers.csv", 
            header=0, index_col=None)
        heifers1['b_date'] = pd.to_datetime(heifers1['b_date'])
        heifers1['age_days'] = (self.today - heifers1['b_date']).dt.days

        self.WY_ids = heifers1['WY_id'].to_list()
        self.heifers = heifers1
                 
        return self.heifers, self.WY_ids
    
    def create_heifer_days(self):
        heifer_age3 = pd.DataFrame(
            columns = self.heifers['WY_id'],
            index=  self.rng                    #creates an array with WY_id cols and datex index
            )
        
        self.date_col = heifer_age3.index.to_list()      
        days2 = days3 = pd.DataFrame()
        WY_ids_integer = [int(i) for i in self.WY_ids]
        
        for i in WY_ids_integer:
            for j in self.date_col:
                start = self.heifers['b_date'][i-1]
                stop = self.today
                
                days_range = pd.date_range(start, stop)
                day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                days1a = pd.DataFrame(day_nums, days_range)
                days1  = days1a.reindex(self.rng)
                
            days2 = pd.concat([days2, days1], axis=1)
        days3 = pd.concat([days3, days2], axis=0)
        days3.columns = self.WY_ids
            
        self.days = days3
        
        return self.days,  self.date_col

    
    def calc_milkdrinking_days(self):
        milk_drinking2=[]
        milk_drinking3 = [] #pd.DataFrame(columns=['WY_id'])
        for i in self.WY_ids:
            
            days = self.days[i]
                
            milk_drinking1a = days.loc[(days > 0) & (days <= 100)].dropna()
            milk_drinking1b = len(milk_drinking1a)
            milk_drinking2.append(milk_drinking1b)
            
        milk_drinking3.append(milk_drinking2)
        self.milk_drinking_days = pd.DataFrame(milk_drinking3, columns=[self.WY_ids])
            
        return self.milk_drinking_days
    
    def calc_TMR_days(self):
        
        for i in self.WY_ids:
            
            days = self.days[i]
            preg_date = self.heifers['preg_date'][i]
            
            start_TMR_growth1 = days[days == 90].index
            stop_TMR_growth1  = days[days == 365].index
            
            start_TMR_growth   = start_TMR_growth1[0]
            stop_TMR_growth = stop_TMR_growth1[0]
            
            start_TMR_growth_regular = stop_TMR_growth + timedelta(days=1)
 
            if preg_date:
                end_date1   = self.heifers['preg_date'][i-1]
                end_date   = pd.to_datetime(end_date1)
            
            elif not preg_date:
                end_date = self.today
            
            
            
            # these create an array of the amounts of TMR/day over the date range

            TMR_growth_range = pd.date_range(start_TMR_growth, stop_TMR_growth)
            len_TMR_growth_range = len(TMR_growth_range)
            TMR_growth_amt_series = np.linspace(.5, 20, len_TMR_growth_range)
            
            TMR_regular_range = pd.date_range(start_TMR_growth_regular,end_date)
            TMR_regular_amt_series = np.full(len(TMR_regular_range), 20)
            
            self.TMR_amt_days = np.concatenate([TMR_growth_amt_series,TMR_regular_amt_series])
            
        return self.TMR_amt_days   
    
    def create_yellow_beans_days(self):
        
        for i in self.WY_ids:
            days = self.days[i]
            start_yellow_beans = days[days == 120].index
            stop_yellow_beans = days[days == 150].index
            yellow_beans_days = pd.date_range(start_yellow_beans, stop_yellow_beans)
            self.yellow_beans_amt_days = np.full(len(yellow_beans_days, 1))
            
        return self.yellow_beans_amt_days
            
            
        
        
        
    
if __name__ == "__main__":
    Heifers()