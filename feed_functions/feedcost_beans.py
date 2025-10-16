'''feedcost_beans.py'''

from datetime import datetime
import inspect
import pandas as pd
from container import get_dependency
from milk_basics import MilkBasics
from date_range import DateRange



class Feedcost_beans:
    def __init__(self):
        print(f"Feedcost_beans instantiated by: {inspect.stack()[1].filename}")
        print(f"🔍 {self.__class__.__module__}: Current stack:")
        for i, frame in enumerate(inspect.stack()[:5]):
            print(f"   {i}: {frame.filename}:{frame.lineno} in {frame.function}")


        self.MB = MilkBasics()
        self.DR = DateRange()
        self.FCB    = get_dependency('feedcost_basics')
        self.SG     = get_dependency('status_groups')
  
        
        self.bid1   = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_invoice_data\\beans_invoice_detail.csv")
        price_seq1  = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_csv\\beans_price_seq.csv")

        start_date      = get_dependency('start_date')
        today           = datetime.today().date()
        self.dateRange  = pd.date_range(start_date,today)
        
        self.price_seq       = price_seq1.loc[:, ['datex', 'unit_price']].set_index('datex')
        self.price_seq.index = pd.to_datetime(self.price_seq.index)
        
        self.herd_daily         = get_dependency('herd_daily')
        self.all_groups_count   = get_dependency('all_groups_count')
        
        self.daily_amt_beans          = self.create_daily_amt_beans()
        self.daily_price_seq_beans    = self.create_daily_price_seq_beans()        
        self.cost_sequence_beans      = self.create_cost_sequence_beans()

        self.write_to_csv()
        

    def create_daily_amt_beans(self):
        daily_amt= self.FCB.feed_series_dict['beans']['dad']
        groups_count= self.all_groups_count
        herd_daily_dry = self.herd_daily[['dry']]  # Get only the 'dry' column
        daily_amt = pd.concat([daily_amt, groups_count, herd_daily_dry], axis=1).fillna(0) 
        
        daily_amt['fresh_amt']   = daily_amt['fresh_kg']      * daily_amt['fresh']
        daily_amt['group_a_amt'] = daily_amt['group_a_kg']    * daily_amt['groupA']
        daily_amt['group_b_amt'] = daily_amt['group_b_kg']    * daily_amt['groupB']
        daily_amt['dry_amt']     = daily_amt['dry_kg']        * daily_amt['dry']

        daily_amt['total_amt'] = daily_amt[['fresh_amt'
            ,'group_a_amt','group_b_amt','dry_amt']].sum(axis=1)
        
        self.daily_amt_beans = daily_amt
                
        return self.daily_amt_beans


    def create_daily_price_seq_beans(self):
        self.daily_price_seq_beans= self.price_seq.reindex(self.dateRange).ffill()
        return self.daily_price_seq_beans
        
        
    def create_cost_sequence_beans(self):
        dcs1 = pd.concat(( self.daily_amt_beans, self.daily_price_seq_beans), axis=1)
        dcs1['daily cost'] = dcs1['unit_price'] * dcs1['total_amt']
        
        self.cost_sequence_beans = dcs1
        return self.cost_sequence_beans
        
             
    def write_to_csv(self):
        self.cost_sequence_beans  .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_beans.csv')
        self.daily_amt_beans      .to_csv('F:\\COWS\\data\\feed_data\\feed_consumption\\daily_amt_beans.csv')
        
if __name__ == "__main__" :
    FPD = Feedcost_beans()
