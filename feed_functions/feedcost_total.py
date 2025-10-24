'''feed_functions.feedcost_total.py'''

import inspect
import pandas as pd
from container import get_dependency
from persistent_container_service import ContainerClient

class Feedcost_total:
    def __init__(self):
        print(f"Feedcost_total instantiated by: {inspect.stack()[1].filename}")
        self.bean_cost = None
        self.cassava_cost = None
        self.corn_cost = None
        self.bypassfat_cost = None
        self.feedcost = None
        self.total_feedcost_details_last = None
        self.total_feedcost_monthly = None

    def load_and_process(self):
        client = ContainerClient()
        self.bean_cost      = client.get_dependency('feedcost_beans')
        self.cassava_cost   = client.get_dependency('feedcost_cassava')
        self.corn_cost      = client.get_dependency('feedcost_corn')
        self.bypassfat_cost = client.get_dependency('feedcost_bypass_fat')

        self.feedcost, self.total_feedcost_details_last = self.create_feedcost_total()
        self.total_feedcost_monthly = self.create_monthly()
        self.write_to_csv()
    
    def create_feedcost_total(self):
        
        beans   = self.bean_cost.cost_sequence_beans        .loc[:, ["daily cost"]]
        cassava = self.cassava_cost.cost_sequence_cassava   .loc[:, ["daily cost"]]
        corn    = self.corn_cost.cost_sequence_corn         .loc[:, ["daily cost"]]
        bypass  = self.bypassfat_cost.cost_sequence_bypass_fat.loc[:, ["daily cost"]]

        beans   .rename(columns={'daily cost': 'beans daily cost'}  , inplace=True)
        cassava .rename(columns={'daily cost': 'cassava daily cost'}, inplace=True)
        corn    .rename(columns={'daily cost': 'corn daily cost'}   , inplace=True)
        bypass  .rename(columns={'daily cost': 'bypass daily cost'} , inplace=True)
        
        tfc = pd.concat((beans, cassava, corn, bypass), axis=1)
        # tfc.index = beans.index  # Ensure the index is preserved
        tfc['total feedcost'] = tfc.sum(axis=1)
        self.feedcost = tfc
        
        beans_last      = beans    .iloc[-2:-1, : ].copy()
        cassava_last    = cassava  .iloc[-2:-1, : ].copy()
        corn_last       = corn     .iloc[-2:-1, : ].copy()
        bypass_last     = bypass   .iloc[-2:-1, : ].copy()
        
        tfc_details     = pd.concat((beans_last, cassava_last, corn_last, bypass_last), axis=0)
        
        self.total_feedcost_details_last = tfc_details

        return self.feedcost, self.total_feedcost_details_last
    
    
    
    
    def create_monthly(self):
        fct = self.feedcost['total feedcost'].to_frame()
        fct.index = pd.to_datetime(fct.index)
        fct['year'] = fct.index.year
        fct['month'] = fct.index.month
        
        fctm1 = fct.groupby(['year','month']).agg(
           { 'total feedcost' : 'sum'}
        )
        
        self.total_feedcost_monthly = fctm1
        
        return self.total_feedcost_monthly

    
    def write_to_csv(self):
        
        self.feedcost                   .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcost.csv')
        self.total_feedcost_details_last.to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\total_feedcost_details_last.csv')
        self.total_feedcost_monthly     .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\total_feedcost_monthly.csv')
        
if __name__ == "__main__":
    Main = Feedcost_total()