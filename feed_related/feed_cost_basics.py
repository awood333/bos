'''feed_cost_basics.py'''

import pandas as pd

class FeedCostBasics:
    def __init__(self):
        startdate = pd.to_datetime('6/1/2023')
        enddate     = pd.to_datetime('9/1/2024')
        self.rng = pd.date_range(startdate, enddate, freq='D')
        
        self.corn_price_seq = self.create_corn_cost_series()
        self.cassava_price_seq = self.create_cassava_cost_series()
        
    def create_corn_cost_series(self):
        cps = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\corn_price_seq.csv', header=0)
        cps1 = cps.copy()
        cps1['datex'] = pd.to_datetime(cps1['datex'])
        cps1.set_index(['datex'], inplace=True)
        
        self.corn_price_seq = cps1.reindex(self.rng, method='ffill').bfill()
        return self.corn_price_seq    
         
         
    def create_cassava_cost_series(self):
        cassava_price_seq = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\cassava_price_seq.csv', header=0)
        cps1 = cassava_price_seq.copy()
        cps1['datex'] = pd.to_datetime(cps1['datex'])
        cps1.set_index(['datex'], inplace=True)
        
        self.cps = cps1.reindex(self.rng, method='ffill').bfill()
        return self.cps
        
        
        
if __name__ == "__main__":
    FBB = FeedCostBasics()
    FBB.create_cassava_cost_series()
                 