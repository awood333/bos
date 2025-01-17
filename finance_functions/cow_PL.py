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
        self.feed_cost_A    =  self.FCB.current_feed_cost['group_a_cost'].loc['sum']
        self.feed_cost_B    =  self.FCB.current_feed_cost['group_b_cost'].loc['sum']        
        self.feed_cost_dry  =  self.FCB.current_feed_cost['dry_cost'].loc['sum']        
        
        self.mask_int = self.SD.milker_ids[-1].copy().astype(int).to_list()
        self.mask_str = self.SD.milker_ids[-1].copy().astype(str).to_list()
        now = datetime.now()
        self.today = pd.to_datetime(now)
        
        self.group_days     = self.get_days_in_groups()
        self.bd2            = self.create_possible_days()
        self.group_table     = self.create_dry_days()
        self.group_table    = self.create_cost()
   
        self.revenue        = self.create_revenue()
        self.net_revenue = self.create_net_revenue()
     
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
       
        x1 = self.group_days.merge(self.bd2, 
                how='left', left_index=True, right_index=True)
        
        x2= x1.drop(columns=['birth_date','death_date','dam_num','arrived','adj_bdate', 'typex', 'readex'])
        
        x2['dry_days'] = x2['possible_days'] - (x2['milking days'])
        self.group_table = x2
        
        return self.group_table
    
    
    def create_cost(self):
        
        gt= self.group_table.copy()
        gt['cost A'] = gt['A count'] * self.feed_cost_A
        gt['cost B'] = gt['B count'] * self.feed_cost_B
        gt['cost dry'] = gt['dry_days'] * self.feed_cost_dry
        gt['total feedcost'] = gt['cost A'] + gt['cost B'] + gt['cost dry']
        
        self.group_table = gt
        return self.group_table
        
        
    def create_revenue(self):        
        
        milk1 = self.MB.data['milk']
        milk2 = milk1.loc[:,self.mask_str].copy()
        rev = (milk2.sum(axis=0) ) * 22
        
        rev.name = 'revenue'
        self.revenue = pd.DataFrame(rev, columns=['revenue'])
        # self.revenue.index = self.revenue.index.astype(str)
        
        return self.revenue
    
    def create_net_revenue(self):
        
        self.group_table = self.group_table .reset_index().rename(columns={'index': 'WY_id'})
        self.revenue = self.revenue          .reset_index().rename(columns={'index': 'WY_id'})
        
        nr1 = pd.concat([self.revenue, self.group_table ], axis=1)
        
        nr1['net revenue'] = nr1['revenue'] - nr1['total feedcost']
        self.net_revenue = nr1
        
        return self.net_revenue
        
    
    def write_to_csv(self):
        self.net_revenue.to_csv('F:\\COWS\\data\\PL_data\\net_revenue\\net_revenue_from_cowPL.csv')   
        
        
        
    

if __name__ == "__main__":
    CowPL()