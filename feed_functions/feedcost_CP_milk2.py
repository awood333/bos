'''Feedcost_CP_milk2.py'''

from datetime import datetime
import inspect
import pandas as pd
from container import get_dependency


class Feedcost_CP_milk2:
    def __init__(self):
        print(f"Feedcost_CP_milk2 instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.FCB = None
        self.SG = None
        self.SD = None
        self.bid1 = None
        self.price_seq = None
        self.dateRange = None
        self.herd_daily = None
        self.all_groups_count = None
        self.daily_amt_CP_milk2 = None
        self.daily_price_seq_CP_milk2 = None
        self.cost_sequence_CP_milk2 = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.FCB= get_dependency('feedcost_basics')
        self.SG = get_dependency('model_groups')
        self.SD = get_dependency('status_data')

        self.bid1 = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_invoice_data\\CP_milk2_invoice_detail.csv")
        price_seq1 = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_csv\\CP_milk2_price_seq.csv")

        self.dateRange = self.DR.date_range_daily
        self.price_seq = price_seq1.loc[:, ['datex', 'unit_price']].set_index('datex')
        self.price_seq.index = pd.to_datetime(self.price_seq.index)

        self.herd_daily = self.SD.herd_daily
        self.all_groups_count = self.SG.all_groups_count

        self.daily_amt_CP_milk2 = self.create_daily_amt_CP_milk2()
        self.daily_price_seq_CP_milk2 = self.create_daily_price_seq_CP_milk2()
        self.cost_sequence_CP_milk2 = self.create_cost_sequence_CP_milk2()

        self.write_to_csv()

        

    def create_daily_amt_CP_milk2(self):
        daily_amt= self.FCB.feed_series_dict['CP_milk2']['dad']
        groups_count= self.all_groups_count
        herd_daily_dry = self.herd_daily[['dry']]  # Get only the 'dry' column
        daily_amt = pd.concat([daily_amt, groups_count, herd_daily_dry], axis=1).fillna(0) 
        
        daily_amt['fresh_amt']   = daily_amt['fresh_kg']      * daily_amt['fresh']
        daily_amt['group_a_amt'] = daily_amt['group_a_kg']    * daily_amt['groupA']
        daily_amt['group_b_amt'] = daily_amt['group_b_kg']    * daily_amt['groupB']
        daily_amt['dry_amt']     = daily_amt['dry_kg']        * daily_amt['dry']

        daily_amt['total_amt'] = daily_amt[['fresh_amt'
            ,'group_a_amt','group_b_amt','dry_amt']].sum(axis=1)
        
        self.daily_amt_CP_milk2 = daily_amt
                
        return self.daily_amt_CP_milk2


    def create_daily_price_seq_CP_milk2(self):
        self.daily_price_seq_CP_milk2= self.price_seq.reindex(self.dateRange).ffill()
        return self.daily_price_seq_CP_milk2
        
        
    def create_cost_sequence_CP_milk2(self):
        dcs1 = pd.concat(( self.daily_amt_CP_milk2, self.daily_price_seq_CP_milk2), axis=1)
        dcs1['daily cost'] = dcs1['unit_price'] * dcs1['total_amt']
        
        self.cost_sequence_CP_milk2 = dcs1
        return self.cost_sequence_CP_milk2
        
             
    def write_to_csv(self):
        self.cost_sequence_CP_milk2  .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_CP_milk2.csv')
        self.daily_amt_CP_milk2      .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\daily_amt_CP_milk2.csv')
        
if __name__ == "__main__" :
    obj = Feedcost_CP_milk2()
    obj.load_and_process()     
