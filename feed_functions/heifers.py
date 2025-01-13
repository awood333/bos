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
        self.align_days()
        
        
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
            
            preg_date = pd.to_datetime(self.heifers.loc[i,'preg_date'])
            
            if not pd.isna(preg_date):  
                end_date1   = preg_date
                end_date   = pd.to_datetime(end_date1)
            
            elif pd.isna(preg_date):
                end_date = self.today
            
            for j in self.date_col:  #defined in constructor
                start = self.heifers.loc[i,'b_date']
                stop = end_date
                
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
        
        TMR_amt_days2=pd.DataFrame()
        
        for i in self.WY_ids:
            heif = self.heifers
            heif = heif.set_index('WY_ids', drop=True)
            days = self.days.loc[:,i]     
            max_days = days.max()
            days_left = max_days - 90
            
            preg_date = pd.to_datetime(heif.loc[i,'preg_date'])
            TMR_kg = 20
            
#cow moves to dry adult cows when preg - preg date should be entered manually and = ultra_date             
            if not pd.isna(preg_date):  
                end_date1   = preg_date
                end_date   = pd.to_datetime(end_date1)
            
            elif pd.isna(preg_date):
                end_date = self.today

            if max_days<90:
                pass
            
            elif days_left >0 :    
                start_TMR_growth1 = days[days == 90].index  #date when calf is 90 days old             
                start_TMR_growth   = start_TMR_growth1.to_pydatetime()[0]   # converts from timestamp to datetime

                if days_left < 180:     #stop date is from 90 + however many days the calf is alive (as of 'today')
                    stop_TMR_growth1 = days[days == (90 + days_left)].index  
                    stop_TMR_growth = stop_TMR_growth1.to_pydatetime()[0]
                
                elif days_left >= 180:  #calf is alive for the full 6months of TMR_growth rations
                    stop_TMR_growth1 = days[days == (90+180)].index  
                    stop_TMR_growth = stop_TMR_growth1.to_pydatetime()[0]
                
                TMR_growth_range = pd.date_range(start_TMR_growth, stop_TMR_growth)
                len_TMR_growth_range = len(TMR_growth_range)
                TMR_growth_amt_series = np.cumsum(np.full(len(TMR_growth_range), 0.11111))
                
                
                
            if max_days >= 271:     # TMR_growth + milkdrinking = 90+180=270
                
                start_TMR_regular1 = days[days == 271].index  
                start_TMR_regular = start_TMR_regular1[0]
                  
                TMR_regular_range = pd.date_range(start_TMR_regular,end_date)
                TMR_regular_amt_series = np.full(len(TMR_regular_range), TMR_kg)

              
            TMR_amt_days1 = np.concatenate([TMR_growth_amt_series,TMR_regular_amt_series], axis=0)

             #convert from numpy to df
            TMR_amt_days1_series = pd.Series(TMR_amt_days1, name=i)
            
            #reindex to start at 90 so that the 90 days of milkdrinking will fit
            TMR_amt_days1_series.index = pd.RangeIndex(start=90, stop= 90 + len(TMR_amt_days1_series) )
            
            TMR_amt_days2 = pd.concat([TMR_amt_days2,TMR_amt_days1_series], axis=1) #stack up
            
            TMR_growth_amt_series = TMR_regular_amt_series = TMR_amt_days1 =  np.array([]) #reinitialize
            
        self.TMR_amt_days = TMR_amt_days2    
        return self.TMR_amt_days   
    
    def create_yellow_beans_days(self):
        
        yellow_beans_amt_days2 = pd.DataFrame()
        
        for i in self.WY_ids:
            
            heif = self.heifers
            heif = heif.set_index('WY_ids', drop=True)
            days = self.days.loc[:,i]     
            max_days = days.max()
            
            if max_days<120:
                pass
            
            elif max_days>=120:
                start1 = days[days == 120].index
                if max_days <150:
                    stop1  = days[days == max_days].index
                    
                elif max_days >=150:
                    stop1 = days[days == 150].index
                
            start = start1[0]
            stop = stop1[0]
            
            yellow_beans_date_range = pd.date_range(start, stop )
            yellow_beans_amt_days1 = np.full(len(yellow_beans_date_range), 1)
            yellow_beans_amt_days_series = pd.Series(yellow_beans_amt_days1, name = i)
            yellow_beans_amt_days2 = pd.concat([yellow_beans_amt_days2, yellow_beans_amt_days_series], axis=1)
            yellow_beans_amt_days1 = np.array([])
        self.yellow_beans_amt_days = yellow_beans_amt_days2
            
        return self.yellow_beans_amt_days
     
    def align_days(self):
        
        new_index = pd.DataFrame()
        
        wy = self.WY_ids
        heif1 = self.heifers
        heif1 = heif1.set_index('WY_ids',drop=True)
        age = heif1['age_days']
        
        for i in wy:
            age1 = age[i]
            age_range = pd.RangeIndex(1,age1)
            
            milk_days1 = self.milk_drinking_days.loc[0,i]
            milk_index = pd.RangeIndex(1, milk_days1, name=i)
            milk_amt = 6
            milk_days1 = pd.Series(milk_amt, index=milk_index)
            milk_days = milk_days1.reindex(age_range, fill_value=0)

            bean_days1 = self.yellow_beans_amt_days[i]
            bean_index = pd.RangeIndex(120, (120+len(bean_days1)), step=1, name=i)
            bean_days1.index = bean_index
            bean_days = bean_days1.reindex(age_range, fill_value=0)

            TMR_days1 = self.TMR_amt_days[i]
            TMR_index = pd.RangeIndex(90, age1, step=1, name=i)
            # TMR_days1.index = TMR_index
            TMR_days = TMR_days1.reindex(age_range, fill_value=0)
            
            new_index = pd.DataFrame( index = age_range)
            days_df = pd.concat([new_index, milk_days, bean_days, TMR_days])
            
        
        return
            
     
if __name__ == "__main__":
    Heifers()