import pandas as pd 
from insem_functions.Insem_ultra_data import InsemUltraData
from milk_functions.milkaggregates  import MilkAggregates

class TendayMilkingDays:
    def __init__ (self):
        
        IUD = InsemUltraData()
        self.MA  = MilkAggregates()
        
        self.days = IUD.allx.loc[:,['WY_id','days milking']]
        self.preg = IUD.allx.loc[:,['WY_id','u_read', 'expected bdate']]
       
        self.td2 =        self.tenday_days()
        
    def tenday_days(self):
        
        td = self.MA.tenday.reset_index()

        # td['pct chg'] = ((td.loc[:,'avg'] / td.loc[:,10])-1)*100
        
        # td1 = pd.merge(td, self.days,
        #               on='WY_id',
        #               how='left')
        
        self.td2 = pd.merge(td, self.preg, on='WY_id', how='left')
        
        
        self.td2     .to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.csv')
        return self.td2
        
if __name__ ==     "__main__"    :
    tenday_days = TendayMilkingDays()
