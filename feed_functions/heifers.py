'''heifers.py'''

import pandas as pd
from datetime import datetime,timedelta
from feed_functions.feed_cost_basics import FeedCostBasics


class Heifers:
    def __init__(self):

        self.FCB = FeedCostBasics()
        self.today = datetime.now()
        self.rng = pd.date_range('2023-01-01', self.today)         
        
        self.age2 = self.get_heifer_age()
        self.days3 = self.create_heifer_days()
        
        
    def get_heifer_age(self):
        today = datetime.now()
        age1 = pd.read_csv("F:\\COWS\\data\\csv_files\\heifers.csv", 
            header=0, index_col=None)
        age1['b_date'] = pd.to_datetime(age1['b_date'])
        age1['age_days'] = (self.today - age1['b_date']).dt.days
        self.age2 = age1
         
        return self.age2
    
    def create_heifer_days(self):
        heifer_age3 = pd.DataFrame(
            columns = self.age2['WY_id'],
            index=  self.rng
            )
        
        WY_ids = self.age2.index.to_list()
        date_col = heifer_age3.index.to_list()
        days2 = days3 = pd.DataFrame()
        
        for i in WY_ids:
            for j in date_col:
                start = self.age2['b_date'][i]
                stop = self.today
                
                days_range = pd.date_range(start, stop)
                day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                days1a = pd.DataFrame(day_nums, days_range)
                days1  = days1a.reindex(self.rng)
                
            days2 = pd.concat([days2, days1], axis=1)
        days3 = pd.concat([days3, days2], axis=0)
            
        self.days = days3
        
        
        return self.days
        
        
        
    
if __name__ == "__main__":
    Heifers()