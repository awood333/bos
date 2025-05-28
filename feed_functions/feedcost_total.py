import pandas as pd


class Feedcost_total:
    def __init__(self):
        
        
        self.feedcost, self.total_feedcost_details_last =  self.create_feedcost_total()
        self.total_feedcost_monthly                     = self.create_monthly()
        self.write_to_csv()
    
    def create_feedcost_total(self):
        
        beans1      = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_beans.csv", index_col=0)
        cassava1    = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_cassava.csv", index_col=0)
        corn1       = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_corn.csv", index_col=0)
        bypass1     = pd.read_csv("F:\\COWS\\data\\feed_data\\feed_consumption\\cost_sequence_bypass_fat.csv", index_col=0)

        
        beans   = beans1    .loc[:, ["daily cost"]]
        cassava = cassava1  .loc[:, ['daily cost']]
        corn    = corn1     .loc[:, ['daily cost']]
        bypass  = bypass1   .loc[:, ['daily cost']]
        
        beans.rename(columns={'daily cost': 'beans daily cost'}, inplace=True)
        cassava.rename(columns={'daily cost': 'cassava daily cost'}, inplace=True)
        corn.rename(columns={'daily cost': 'corn daily cost'}, inplace=True)
        bypass.rename(columns={'daily cost': 'bypass daily cost'}, inplace=True)
        
        tfc = pd.concat((beans, cassava, corn, bypass), axis=1)
        # tfc.index = beans.index  # Ensure the index is preserved
        tfc['total feedcost'] = tfc.sum(axis=1)
        self.feedcost = tfc
        
        beans_last      = beans1    .iloc[-2:-1, : ].copy()
        cassava_last    = cassava1  .iloc[-2:-1, : ].copy()
        corn_last       = corn1     .iloc[-2:-1, : ].copy()
        bypass_last     = bypass1   .iloc[-2:-1, : ].copy()
        
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