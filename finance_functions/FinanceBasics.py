'''finance_functions.FinanceBasics.py'''

import pandas as pd

from feed_functions.feedcost_basics import Feedcost_basics

class FinanceBasics:
    def __init__(self):

        self.fc  = Feedcost_basics()
        bkk = pd.read_csv('F:\\COWS\\data\\finance\\BKKbank\\BKKBankFarmAccount.csv', index_col='datex')
        bkk.index = pd.to_datetime(bkk.index, format="%Y-%m-%d")
        self.bkk1 = bkk.iloc[:,:12]
        
        
        self.startdate = pd.to_datetime("2024-07-01") 
        self.stopdate  = pd.to_datetime("2024-12-31") 
          
        self.idx       = pd.date_range(self.startdate, self.stopdate, freq='D')

        # functions
        self.cost_df     = self.create_cost_df()
        self.cost_xfeed_pivot = self.create_cost_xfeed_pivot()
        self.feedcost_pivot   = self.create_feedcost_pivot()
        self.create_write_to_csv() 
        
        
    def create_cost_df(self):
        
        bkk2 = self.bkk1[   
            (
                (self.bkk1['year'] >= 2024 )
                & (self.bkk1['month'] >= 1)
            )  ].copy()
        
        bkk2  = bkk2.set_index(['year', 'month'])
        bkk2  = bkk2.reset_index()
        
        
        bkk3 = bkk2.drop(columns=['day','transaction',
            'credit','descr 3','capex'])
        
        bkk3  = bkk3[bkk3['descr 1'].notna()].copy()
        bkk3['debit']  = pd.to_numeric(bkk3['debit'], errors='coerce')
        self.cost_df = bkk3
        
        return self.cost_df
    
    
    def create_cost_xfeed_pivot(self):
        
        bkk4 = pd.pivot_table(
            self.cost_df, 
            index = ['year','month'],
            values = 'debit',
            columns= 'descr 1',
            aggfunc= 'sum'
            )
        
        bkk4 = bkk4.reset_index()
        self.bkk3 = bkk4
        
        bkk5 = bkk4.drop(columns=['sale',  'feed']) 
        bkk5.loc     [:,'total cost']    = bkk5.sum(axis = 1)    # last col

        self.cost_xfeed_pivot = bkk5
        
        return self.cost_xfeed_pivot


    def create_feedcost_pivot(self):
        
        bkk6 = self.cost_df
        bkk7 = bkk6.loc[(bkk6['descr 1'] == 'feed')]
        
        bkk8 = bkk7.drop(columns=['descr 1']) 
        
        bkk9 = pd.pivot_table(
            bkk8, 
            index = ['year','month'],
            values = 'debit',
            columns= 'descr 2',
            aggfunc= 'sum'
            )
        self.feedcost_pivot = bkk9
        
        return self.feedcost_pivot


    
    def create_write_to_csv(self):
        
        self.feedcost_pivot     .to_csv('F:\\COWS\\data\\finance\\BKKbank\\feedcost_pivot.csv')
        self.cost_xfeed_pivot   .to_csv('F:\\COWS\\data\\finance\\BKKbank\\cost_xfeed_pivot.csv')
        
    
if __name__ == "__main__":
    FinanceBasics()
