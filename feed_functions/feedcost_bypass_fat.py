'''feedcost_byoass_fat.py'''

from datetime import datetime
import pandas as pd

from CreateStartDate import DateRange
from feed_functions.feedcost_basics import Feedcost_basics
from status_functions.status_groups import statusGroups
from status_functions.statusData import StatusData

class Feedcost_bypass_fat:
    def __init__ (self):
        
        DR = DateRange()
        SD          = StatusData()
        self.FCB    = Feedcost_basics()
        self.SG     = statusGroups()
        price_seq1  = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_csv\\bypass_fat_price_seq.csv")

        start_date      = DR.start_date()
        today           = datetime.today().date()
        self.dateRange  = pd.date_range(start_date,today)
        
        self.price_seq       = price_seq1.loc[:, ['datex', 'unit_price']].set_index('datex')
        self.price_seq.index = pd.to_datetime(self.price_seq.index)
        
        self.herd_daily         = SD.herd_daily
        self.all_groups_count   = self.SG.all_groups_count
        
        self.daily_amt_bypass_fat          = self.create_daily_amt_bypass_fat()
        self.daily_price_seq_bypass_fat    = self.create_daily_price_seq_bypass_fat()        
        self.cost_sequence_bypass_fat      = self.create_cost_sequence_bypass_fat()

        self.write_to_csv()
        

    def create_daily_amt_bypass_fat(self):
        daily_amt= self.FCB.feed_series_dict['bypass_fat']['dad']
        groups_count= self.all_groups_count
        daily_amt = pd.concat((daily_amt, groups_count, self.herd_daily['dry']), axis=1).fillna(0) 
        
        daily_amt['fresh_amt']   = daily_amt['fresh_kg']      * daily_amt['fresh']
        daily_amt['group_a_amt'] = daily_amt['group_a_kg']    * daily_amt['groupA']
        daily_amt['group_b_amt'] = daily_amt['group_b_kg']    * daily_amt['groupB']
        daily_amt['dry_amt']     = daily_amt['dry_kg']        * daily_amt['dry']

        daily_amt['total_amt'] = daily_amt[['fresh_amt'
            ,'group_a_amt','group_b_amt','dry_amt']].sum(axis=1)
        
        self.daily_amt_bypass_fat = daily_amt
                
        return self.daily_amt_bypass_fat


    def create_daily_price_seq_bypass_fat(self):
        self.daily_price_seq_bypass_fat= self.price_seq.reindex(self.dateRange).ffill()
        return self.daily_price_seq_bypass_fat
        
        
    def create_cost_sequence_bypass_fat(self):
        dcs1 = pd.concat(( self.daily_amt_bypass_fat, self.daily_price_seq_bypass_fat), axis=1)
        dcs1['daily cost'] = dcs1['unit_price'] * dcs1['total_amt']
        
        self.cost_sequence_bypass_fat = dcs1
        return self.cost_sequence_bypass_fat
        
             
    def write_to_csv(self):
        self.cost_sequence_bypass_fat  .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_bypass_fat.csv')
        self.daily_amt_bypass_fat      .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\daily_amt_bypass_fat.csv')
        
if __name__ == "__main__" :
    FPD = Feedcost_bypass_fat()
