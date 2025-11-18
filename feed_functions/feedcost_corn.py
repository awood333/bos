'''feedcost_corn.py'''

import inspect
from datetime import datetime
import pandas as pd
from container import get_dependency


class Feedcost_corn:
    def __init__(self):
        print(f"Feedcost_corn instantiated by: {inspect.stack()[1].filename}")

        self.MB = None
        self.DR = None
        self.SD = None
        self.FCB = None
        self.SG = None
        self.price_seq = None
        self.dateRange = None
        self.herd_daily = None
        self.all_groups_count = None
        self.daily_amt_corn = None
        self.daily_price_seq_corn = None
        self.cost_sequence_corn = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.SD = get_dependency('status_data')
        self.FCB= get_dependency('feedcost_basics')
        self.SG = get_dependency('model_groups')
        price_seq1 = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_csv\\corn_price_seq.csv")

        self.dateRange = self.DR.date_range_daily
        self.price_seq = price_seq1.loc[:, ['datex', 'unit_price']].set_index('datex')
        self.price_seq.index = pd.to_datetime(self.price_seq.index)

        self.herd_daily = self.SD.herd_daily
        self.all_groups_count = self.SG.all_groups_count

        self.daily_amt_corn = self.create_daily_amt_corn()
        self.daily_price_seq_corn = self.create_daily_price_seq_corn()
        self.cost_sequence_corn = self.create_cost_sequence_corn()

        self.write_to_csv()

    def create_daily_amt_corn(self):
        daily_amt= self.FCB.feed_series_dict['corn']['dad']
        groups_count= self.all_groups_count
        daily_amt = pd.concat((daily_amt, groups_count, self.herd_daily['dry']), axis=1).fillna(0) 
        
        daily_amt['fresh_amt']   = daily_amt['fresh_kg']      * daily_amt['fresh']
        daily_amt['group_a_amt'] = daily_amt['group_a_kg']    * daily_amt['groupA']
        daily_amt['group_b_amt'] = daily_amt['group_b_kg']    * daily_amt['groupB']
        daily_amt['dry_amt']     = daily_amt['dry_kg']        * daily_amt['dry']
        daily_amt['total_amt']   = daily_amt[['fresh_amt','group_a_amt','group_b_amt','dry_amt']].sum(axis=1)
        
        self.daily_amt_corn      = daily_amt       
        return self.daily_amt_corn
    
    
    
    def create_daily_price_seq_corn(self):
        self.daily_price_seq_corn= self.price_seq.reindex(self.dateRange).ffill()
        return self.daily_price_seq_corn
        
        
    def create_cost_sequence_corn(self):
        dcs1 = pd.concat(( self.daily_amt_corn, self.daily_price_seq_corn), axis=1)
        dcs1['daily cost'] = dcs1['unit_price'] * dcs1['total_amt']
        
        self.cost_sequence_corn = dcs1
        return self.cost_sequence_corn
        
        
    def write_to_csv(self):
        self.cost_sequence_corn  .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_corn.csv')
        self.daily_amt_corn      .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\daily_amt_corn.csv')
        
if __name__ == "__main__" :
    obj = Feedcost_corn()
    obj.load_and_process()     
