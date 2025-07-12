'''heifer_cost_model.py'''
from datetime import datetime
import pandas as pd
import numpy as np

from feed_functions.feedcost_basics import Feedcost_basics
from insem_functions.Insem_ultra_data import InsemUltraData

class HeiferCostModel:
    def __init__(self):

        self.FCB = Feedcost_basics()
        self.IUD = InsemUltraData()
        
        start_date = pd.to_datetime('2024-01-01').date().isoformat()
        
        now = datetime.now()
        self.today = pd.to_datetime(now)
        self.rng = pd.date_range(start_date, self.today, freq='D')    

        self.milk_price = pd.Series(22.0)   # change this if the amount is changed
        self.milk_liters_day = pd.Series(6) # change this if the amount is changed

        self.kg_progression = pd.DataFrame(np.linspace(6.0, 22.0, 300), columns=['kg'])
        
        # functions
        self.day_nums_df            = self.create_heifer_days()
        self.feed_price_df          = self.create_daily_feedprice()
        
        self.diet, self.TMR_cost_kg = self.create_daily_feedcost()
        self.milk_drinking_cost     = self.create_milk_drinking()
        self.eating_amount          = self.create_eating_amount()
        self.daily_TMR_cost         = self.create_daily_TMR_cost()
        self.heifer_feedcost_daily  = self.create_total_feedcost()
        self.heifer_feedcost_monthly = self.create_feedcost_monthly()
        

        self.write_to_csv()
        
    def create_heifer_days(self):
        
        bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\heifers_birth_death.csv', index_col=0)
        bd1['b_date'] = pd.to_datetime(bd1['b_date'], errors='coerce')

        bd2 = bd1['adj_bdate'].to_frame()        
        day_nums_df = pd.DataFrame(index=self.rng)
        
        for i in bd2.index:
            
            date = bd2.loc[i,'adj_bdate']

            days_range = pd.date_range(date, self.today)
            day_nums1 = pd.Series(range(1,len(days_range)+1), index=days_range, name=i)
            day_nums = day_nums1.reindex(self.rng)
            
            day_nums_df[i] = day_nums

        self.day_nums_df = day_nums_df
        
        return self.day_nums_df
    
    def create_daily_feedprice(self):
        
        fsd1 = self.FCB.feed_series_dict
        feed_type = self.FCB.feed_type
        
        psd_dict = {feed_type: v['psd'] for feed_type, v in fsd1.items() if 'psd' in v}
        
        feed_price_dict = {}
        for feed_type, df in psd_dict.items():
            # Assumes 'unit_price' column exists and index is date
            feed_price_dict[feed_type] = df['unit_price']

        # Combine into a single DataFrame (columns=feed_type, index=date)
        self.feed_price_df = pd.DataFrame(feed_price_dict)
        
        return self.feed_price_df
    
    
    def create_daily_feedcost(self):
        diet1 = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\heifer_daily_amt.csv')
        diet1['datex'] = pd.to_datetime(diet1['datex'], errors='coerce')
        diet1 = diet1.set_index('datex')
        diet2 = diet1.reindex(self.rng).ffill()
        
        fpd = self.feed_price_df
        diet3 = pd.concat([diet2, fpd], axis=1)
        diet3['corn cost']      = diet3['corn kg'] * diet3['corn']
        diet3['cassava cost']   = diet3['cassava kg'] * diet3['cassava']
        diet3['beans cost']     = diet3['beans kg'] * diet3['beans']
        diet3['straw cost']     = diet3['straw kg'] * diet3['straw']
        diet3['bypass_fat cost']= diet3['bypass_fat kg'] * diet3['bypass_fat']
        # diet3['premix cost']    = diet3['premix kg'] * diet3['premix']
        # diet3['baking soda cost']    = diet3['baking_soda kg'] * diet3['baking_soda']
        diet3['total daily feedcost']= diet3.sum(axis=1)        
        
                
        # create diet/kg
        self.TMR_cost_kg = (diet3['total daily feedcost']) /  diet3['total kg']
        
        self.diet = diet3

        
        return self.diet, self.TMR_cost_kg
    
    def create_milk_drinking(self):
        
        dnf = self.day_nums_df.T
        
        milk_drinking_cost = {}
        
        for i in dnf.index:
            days = dnf.loc[i]       #i = cols for each heifer
            arr = np.zeros(len(days))
            milk_cost = 6*22
            
            mask1 = (days >0) & (days <= 75)
            mask2 = (days >75)
            arr = np.select([mask1, mask2], [milk_cost, 0] , default=np.nan)
            milk_drinking_cost[i] = pd.Series(arr, index=days.index)
            
        self.milk_drinking_cost = pd.DataFrame(milk_drinking_cost, index=self.day_nums_df.index)
            
        return self.milk_drinking_cost
    
    
    def create_eating_amount(self):

        dnf_T = self.day_nums_df.T
        eating_amounts={}
        
        for i in dnf_T.index:
            days = dnf_T.loc[i]
            arr = np.zeros(len(days))
            
           
            mask1 = (days > 0 ) & (days <= 75)
            mask2 = (days > 75) & (days <= 300)
            mask3 = (days > 300)
            
            arr = np.select([mask1, mask3], [0, 22], default=np.nan)

            # fill progression for mask2
            prog_len = mask2.sum()
            if prog_len > 0:
                arr[mask2] = self.kg_progression['kg'].values[:prog_len]

            eating_amounts[i] = pd.Series(arr, index=days.index)
        
        eating_amounts_df = pd.DataFrame(eating_amounts, index=self.day_nums_df.index)
        self.eating_amount = eating_amounts_df
        return self.eating_amount
    
    def create_daily_TMR_cost(self):
        
        tck = self.TMR_cost_kg
        ea  = self.eating_amount

        self.daily_TMR_cost = ea.mul(tck, axis=0)
        
        return self.daily_TMR_cost


    def create_total_feedcost(self):
        
        dtc = self.daily_TMR_cost
        mdc = self.milk_drinking_cost
        total_cost  = dtc.add(mdc)
        
        self.heifer_feedcost_daily = total_cost
        return self.heifer_feedcost_daily
        
        
    def create_feedcost_monthly(self):
        tfc1 = self.heifer_feedcost_daily
        tfc1['year']  = tfc1.index.year
        tfc1['month'] = tfc1.index.month
        
        tfc2 = tfc1.groupby(['year', 'month']).sum()
        tfc2['heifer_cost'] = tfc2.sum(axis=1)
        
        self.heifer_feedcost_monthly = tfc2
        
        return self.heifer_feedcost_monthly
    

        
    
    def write_to_csv(self):
        
        self.heifer_feedcost_daily  .to_csv("F:\\COWS\\data\\feed_data\\heifers\\heifer_cost_daily.csv")
        self.heifer_feedcost_monthly.to_csv("F:\\COWS\\data\\feed_data\\heifers\\heifer_cost_monthly.csv")
        return
            
            
            
if __name__ == "__main__":
    HeiferCostModel()            
        
                