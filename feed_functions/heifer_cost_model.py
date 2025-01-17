'''heifer_cost_model.py'''

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from feed_functions.feed_cost_basics import FeedCostBasics
from insem_functions.Insem_ultra_data import InsemUltraData

class Heifers:
    def __init__(self):

        self.FCB = FeedCostBasics()
        self.IUD = InsemUltraData()
        
        
        #feed costs
        self.dry_feed_cost      = self.FCB.current_feed_cost['dry_cost'].loc['sum']
        self.dry_feed_kg        = self.FCB.current_feed_cost['dry_kg'].loc['sum']
        self.dry_feed_cost_kg   = self.dry_feed_cost / self.dry_feed_kg
        self.TMR_costper_kg     = self.dry_feed_cost / self.dry_feed_kg
        self.bean_cost          = self.FCB.current_feed_cost['unit_price'].loc['beans']

        self.milk_price = pd.Series(22.0)
        self.milk_liters_day = pd.Series(6)
        
        now = datetime.now()
        self.today = pd.to_datetime(now)

        self.days_to_sale = 365+365+60  #days to 2months before calving...based on insem at 18m
        self.days_to_calf = 365+365+90 #820 this is cut-off for feed calc
        self.rng_to_sale = pd.RangeIndex(1, self.days_to_sale)
        self.rng_to_calf = pd.RangeIndex(1, self.days_to_calf)
        
        self.heifer = self.create_milk_drinking_days()
        self.heifer, self.len_TMR_growth = self.create_TMR_growth_days()
        self.heifer = self.create_TMR_regular_days()
        self.heifer = self.create_yellow_bean_days()
        self.heifer, self.heifer_monthly = self.create_total_costs()
        self.write_to_csv()
        
    def create_milk_drinking_days(self):
        
        self.heifer = pd.DataFrame(index=self.rng_to_calf)
        milk_drinking_range =  pd.RangeIndex(0,90)
        milk_cost = self.milk_liters_day * self.milk_price
        milk_drinking_series = pd.Series( milk_cost.values[0], index=milk_drinking_range, name='milk')
        milk_drinking_series = milk_drinking_series.reindex(self.rng_to_calf, fill_value=0)
        
        self.heifer['milk drinking'] = milk_drinking_series
        return self.heifer
        
            
    def create_TMR_growth_days(self):
        TMR_growth_range = pd.RangeIndex(60, 365)
        growth_len = len(TMR_growth_range)

        amt_array = np.linspace(.1, self.dry_feed_kg, num=growth_len)
        cost_array = amt_array * self.TMR_costper_kg
        
        TMR_growth_cost_series= pd.Series(cost_array)
        TMR_growth_cost_series.index = pd.RangeIndex(start=60, stop=60 + len(TMR_growth_cost_series))
        self.len_TMR_growth = len(TMR_growth_cost_series)       
        
        self.heifer['TMR_growth'] = TMR_growth_cost_series
        
        return self.heifer, self.len_TMR_growth
    
    
    def create_TMR_regular_days(self):        
        TMR_regular_range = pd.RangeIndex(365, self.days_to_calf)
        regular_len = len(TMR_regular_range)        
        
        amt_array = np.full(shape=regular_len, fill_value=self.dry_feed_kg, dtype=float)
        cost_array = amt_array * self.TMR_costper_kg
        
        TMR_regular_cost_series= pd.Series(cost_array)
        TMR_regular_cost_series.index = pd.RangeIndex(start=365, stop=365 + regular_len)       
        
        self.heifer['TMR_regular'] = TMR_regular_cost_series
        
        return self.heifer
    
    
    def create_yellow_bean_days(self):
        
        bean_range = pd.RangeIndex(90, 120)
        bean_len = len(bean_range)
        bean_amt = 1        #NOTE: if amount changes need to change here manually
        
        amt_array = np.full(shape=30, fill_value=bean_amt, dtype=float)
        cost_array = amt_array * self.bean_cost
        
        bean_cost_series= pd.Series(cost_array)        
        bean_cost_series.index = pd.RangeIndex(start=60, stop=(30 + 60))    
        self.heifer['beans'] = bean_cost_series        
        
        return self.heifer
    
    def create_total_costs(self):
        
        self.heifer['30_day_segment'] = (self.heifer.index // 30) + 1
        self.heifer_monthly = self.heifer.groupby('30_day_segment').sum()
        self.heifer_monthly['cost/month'] = self.heifer_monthly.sum(axis=1)
        self.heifer_monthly.loc['feedtype sum']   = self.heifer_monthly.sum(axis=0)

        self.heifer['cost/day'] = self.heifer.sum(axis=1)
        self.heifer.loc['feedtype sum'] = self.heifer.sum(axis=0)
        self.heifer['cum sum']  = self.heifer['cost/day'].cumsum()

        
        return self.heifer, self.heifer_monthly
    

        
    
    def write_to_csv(self):
        
        self.heifer.to_csv("F:\\COWS\\data\\feed_data\\heifers\\heifer_model.csv")
        self.heifer_monthly.to_csv("F:\\COWS\\data\\feed_data\\heifers\\heifer_model_monthly.csv")
            
            
            
if __name__ == "__main__":
    Heifers()            
        
                