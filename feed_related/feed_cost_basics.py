'''feed_cost_basics.py'''

import os
from datetime import datetime
import pandas as pd

from CreateStartDate import DateRange 

class FeedCostBasics:
    def __init__(self):
        self.rng = DateRange().date_range
        self.feed_type =  ['corn','cassava','beans','straw'] 

        self.price_seq_dict, self.daily_amt_dict = self.create_feed_cost()
        
        [
        self.corn_psd,
        self.cassava_psd,
        self.beans_psd,
        self.straw_psd, 
        
        self.corn_dad,
        self.cassava_dad,
        self.beans_dad,
        self.straw_dad
        ]                       = self.create_separate_feed_series()
            

    def create_feed_cost(self):
        
            
        base_path = 'F:/COWS/data/feed_data/feed_csv'
        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        
        for feed in self.feed_type:        
            price_seq_path = os.path.join(base_path, f'{feed}_price_seq.csv')
            daily_amt_path = os.path.join(base_path, f'{feed}_daily_amt.csv')

            price_seq   = pd.read_csv(price_seq_path,  header=0, index_col=0)  
            daily_amt   = pd.read_csv(daily_amt_path,  header=0, index_col=0)  
        
            price_seq.index = pd.to_datetime(price_seq.index)                 
            price_seq = price_seq.reindex(self.rng, method='ffill')


            daily_amt.index = pd.to_datetime(daily_amt.index)                             
            daily_amt = daily_amt.reindex(self.rng, method='ffill').bfill()
            
            self.price_seq_dict[feed] = price_seq
            self.daily_amt_dict[feed] = daily_amt
            
        return self.price_seq_dict, self.daily_amt_dict
    
    def create_separate_feed_series (self):
        psd = self.price_seq_dict
        dad = self.price_seq_dict
        self.corn_psd       = psd['corn']
        self.cassava_psd    = psd['cassava']
        self.beans_psd      = psd['beans']
        self.straw_psd      = psd['straw'] 
        
        self.corn_dad       = dad['corn']
        self.cassava_dad    = dad['cassava']
        self.beans_dad      = dad['beans']
        self.straw_dad      = dad['straw']
        
        return  [
        self.corn_psd,
        self.cassava_psd,
        self.beans_psd,
        self.straw_psd, 
        
        self.corn_dad,
        self.cassava_dad,
        self.beans_dad,
        self.straw_dad
        ]
        
       
        
if __name__ == "__main__":
    FBB = FeedCostBasics()
    
                 