'''
cow_PL.py
'''
import pandas as pd
import numpy as np
from datetime import datetime
from milk_functions.WetDry import WetDry
from milk_functions.statusData import StatusData
from MilkBasics import MilkBasics
from feed_functions.feed_cost_basics import FeedCostBasics
from insem_functions.Insem_ultra_data import InsemUltraData

class CowPL:
    def __init__(self):
    
        self.WD = WetDry()
        self.SD = StatusData()
        self.MB = MilkBasics()
        self.FCB = FeedCostBasics()
        self.IUD = InsemUltraData()
        
        
        #feed costs
        self.dry_feed_cost  = self.FCB.current_feed_cost['dry_cost'].loc['sum']
        self.dry_feed_kg    = self.FCB.current_feed_cost['dry_kg'].loc['sum']
        self.dry_feed_cost_kg = self.dry_feed_cost / self.dry_feed_kg
        self.TMR_costper_kg = self.dry_feed_cost / self.dry_feed_kg
        self.bean_cost  =  self.FCB.current_feed_cost['unit_price'].loc['beans']
        
        self.mask_int = self.SD.milker_ids[-1].copy().astype(int).to_list()
        self.mask_str = self.SD.milker_ids[-1].copy().astype(str).to_list()
        now = datetime.now()
        self.today = pd.to_datetime(now)
        
        self.group_days = self.get_days_in_groups()
        self.bd1 = self.create_possible_days()
        self.group_days = self.create_dry_days()
        self.revenue = self.create_income()
     
        self.write_to_csv()
        
    def get_days_in_groups(self):
        
        wetdays = self.WD.wdd.loc[:,self.mask_int]
        
        daysA_list = []
        daysB_list = []
        self.daysA_df = pd.DataFrame()
        self.daysB_df = pd.DataFrame()
        
        for i in wetdays.columns:
            days = wetdays.loc[:,i]
            daysA = ((days !=0) & (days <= 210)).sum()
            daysB = (days > 210).sum()
  
            
            daysA_list.append(daysA)
            daysB_list.append(daysB)
     
        
        daysA_df = pd.DataFrame(daysA_list, index=self.mask_int, columns=['A count'])  
        daysB_df = pd.DataFrame(daysB_list, index=self.mask_int, columns=['B count']) 
        
        self.group_days =     daysA_df.merge(daysB_df, 
                how='left', left_index=True, right_index=True)       
        self.group_days['milking days'] = self.group_days['A count'] + self.group_days['B count']
        
        return self.group_days
    
    
    def create_possible_days(self):
        
        bd1 = self.MB.data['bd'].copy()
        bd1 = bd1.set_index('WY_id')
        self.bd2 = bd1.loc[self.mask_int, :]
        arrived = self.bd2['arrived']
        
        if not pd.isna (arrived).all():
            self.bd2['possible_days'] = (self.today - arrived).dt.days 
            
        return self.bd2
        
    def create_dry_days(self):
        
        x = self.group_days.merge(self.bd2[['possible_days']], 
                how='left', left_index=True, right_index=True)
        
        x['dry_days'] = x['possible_days'] - (x['milking days'])
        self.group_days = x
        

        return self.group_days
    
    
    def create_cost(self):
        
    def create_income(self):        
        
        milk1 = self.MB.data['milk']
        milk2 = milk1.loc[:,self.mask_str].copy()
        self.revenue = milk2.sum(axis=0)
        return self.revenue
        
    
    def write_to_csv(self):
        self.group_days.to_csv('F:\\COWS\\data\\status\\group_days.csv')   
        
        
        
    

if __name__ == "__main__":
    CowPL()