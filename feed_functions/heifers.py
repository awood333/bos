'''heifers.py'''

import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from feed_functions.feed_cost_basics import FeedCostBasics


class Heifers:
    def __init__(self):

        self.FCB = FeedCostBasics()
        self.today = datetime.now()
        self.rng = pd.date_range('2023-01-01', self.today)
        
   
        
        self.heifers, self.WY_ids,        = self.get_heifer_age()
        self.days3, self.date_col       = self.create_heifer_days()
        
        self.milk_drinking = self.calc_milkdrinking_cost()
        
        
    def get_heifer_age(self):
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
            index=  self.rng
            )
        
        self.date_col = heifer_age3.index.to_list()      
       
        days2 = days3 = pd.DataFrame()
        
        for i in self.WY_ids:
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
    
    def calc_milkdrinking_cost(self):
        milk_drinking2=[]
        milk_drinking3 = [] #pd.DataFrame(columns=['WY_id'])
        for i in self.WY_ids:
            
            days = self.days[i]
                
            milk_drinking1a = days.loc[(days > 0) & (days <= 100)].dropna()
            milk_drinking1b = len(milk_drinking1a)
            milk_drinking2.append(milk_drinking1b)
            
            start_TMR1 = days[days == 100].index
            stop_TMR_growth1  = days[days == 360].index
            
            start_TMR   = start_TMR1[0]
            stop_TMR_growth1 = stop_TMR_growth1[0]
            
            start_TMR_regular = stop_TMR_growth1 + timedelta(days=1)
 
            
            preg_date1   = self.heifers['preg_date'][i-1]
            preg_date   = pd.to_datetime(preg_date1)
            
            TMR_regular_range = pd.date_range(start_TMR_regular,preg_date)
            
            TMR_growth_range = pd.date_range(start_TMR, stop_TMR_growth1)
            len_TMR_growth_range = len(TMR_growth_range)
            TMR_growth_series = np.linspace(2, 20, len_TMR_growth_range)
            
            TMR_regular_series = np.full(len(TMR_regular_range), 20)
            TMR_series = np.concatenate([TMR_growth_series,TMR_regular_series])
            
            
            yellow_bean1a = days.loc[(days > 100)].dropna()
            
            
        milk_drinking3.append(milk_drinking2)
        self.milk_drinking = pd.DataFrame(milk_drinking3, columns=['WY_ids'])
            
        return self.milk_drinking
        
        
        
    
if __name__ == "__main__":
    Heifers()