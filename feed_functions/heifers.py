'''heifers.py'''

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from feed_functions.feed_cost_basics import FeedCostBasics
from insem_functions.Insem_ultra_data import InsemUltraData

class Heifers:
    def __init__(self):

        self.FCB = FeedCostBasics()
        self.IUD = InsemUltraData()
        
        self.dry_feed_cost  = self.FCB.current_feed_cost['dry_cost'].iloc[-1]
        self.dry_feed_kg    = self.FCB.current_feed_cost['dry_kg'].iloc[-1]
        self.dry_TMR_costper_kg = self.dry_feed_cost / self.dry_feed_kg
    
        now = datetime.now()
        self.today = pd.to_datetime(now)
        self.rng = pd.date_range('2023-01-01', self.today)
 
        
        self.heifers, self.WY_ids,        = self.create_heifer_df()
        self.days3, self.date_col       = self.create_heifer_days()
        
        self.milk_drinking_days, self.cost_milk = self.calc_milkdrinking_days()
        self.TMR_amt_days = self.calc_TMR_days()
        self.yellow_beans_amt_days = self.create_yellow_beans_days()
        
        
    def create_heifer_df(self):
        
        heifers1 = pd.read_csv("F:\\COWS\\data\\csv_files\\heifers_birth_death.csv", 
            header=0, index_col=None)
        heifers1['b_date'] = pd.to_datetime(heifers1['b_date'])
        heifers1['age_days'] = (self.today - heifers1['b_date']).dt.days

        heifers1.reset_index()
        self.WY_ids = heifers1['WY_ids'].to_list()
        
        self.heifers = heifers1
                 
        return self.heifers, self.WY_ids
    
    def create_heifer_days(self):
        heifers = pd.DataFrame(
            columns = self.heifers['WY_ids'],
            index=  self.rng                    #creates an array with WY_id cols and datex index
            )
        
        self.date_col = heifers.index.to_list()      
        days2 = days3 = pd.DataFrame()
        
        for i in self.heifers.index: #integer index from 0
            for j in self.date_col:  #defined in constructor
                start = self.heifers.loc[i,'b_date']
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
        cost_milk2 = []
        str_cols = [str(x) for x in self.days.columns]
        
        for i in self.WY_ids:
            
            days = self.days.loc[:,i]
                
            milk_drinking1a = days[ 
                (days > 0) & (days <= 90)
                ].dropna()
            
            milk_drinking1b = len(milk_drinking1a)
            cost_milk1       = milk_drinking1b * 22
            
            milk_drinking2.append(milk_drinking1b)
            cost_milk2.append(cost_milk1)

        
        self.milk_drinking_days = pd.DataFrame([milk_drinking2], columns=self.WY_ids)
        self.cost_milk = pd.DataFrame([cost_milk2], columns=self.WY_ids)    
        return self.milk_drinking_days, self.cost_milk
    
    def calc_TMR_days(self):
        
        for i in self.WY_ids:
            heif = self.heifers
            heif = heif.set_index('WY_ids', drop=True)
            days = self.days.loc[:,i]
            preg_date = pd.to_datetime(heif.loc[i,'preg_date'])
            end_date = self.today
            
            start_TMR_growth1 = days[days == 90].index  #date when calf is 90 days old
            stop_TMR_growth1 = days[days == 365].index
            
            start_TMR_growth   = start_TMR_growth1[0]   # converts from timestamp to datetime
            stop_TMR_growth = stop_TMR_growth1[0]
            
            start_TMR_growth_regular = stop_TMR_growth + timedelta(days=1)
 
            if not pd.isna(preg_date):
                end_date1   = preg_date
                end_date   = pd.to_datetime(end_date1)
            
            elif pd.isna(preg_date):
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