import pandas as pd
from feed_functions.feedcost_beans import Feedcost_beans
from feed_functions.feedcost_cassava import Feedcost_cassava
from feed_functions.feedcost_corn import Feedcost_corn
from feed_functions.feedcost_bypass_fat import Feedcost_bypass_fat

class Feedcost_total:
    def __init__(self, feedcost_beans=None, feedcost_cassava=None, feedcost_corn=None, feedcost_bypass_fat=None):
        
        self.bean_cost      = feedcost_beans or Feedcost_beans()
        self.cassava_cost   = feedcost_cassava or Feedcost_cassava()
        self.corn_cost      = feedcost_corn or Feedcost_corn()
        self.bypassfat_cost = feedcost_bypass_fat or Feedcost_bypass_fat()
        
        self.feedcost, self.total_feedcost_details_last =  self.create_feedcost_total()
        self.total_feedcost_monthly                     = self.create_monthly()
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