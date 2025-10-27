'''finance_functions.FinanceBasics.py'''
import inspect
import pandas as pd

from feed_functions.feedcost_basics import Feedcost_basics

class FinanceBasics:
    def __init__(self, feedcost_basics=None):
        print(f"FinanceBasics instantiated by: {inspect.stack()[1].filename}")
        self.fc = feedcost_basics or Feedcost_basics()
        self.bkk1 = None
        self.startdate = None
        self.stopdate = None
        self.idx = None
        self.cost_df = None
        self.cost_xfeed_pivot = None
        self.feedcost_pivot = None

    def load_and_process(self):
        bkk = pd.read_csv('F:\\COWS\\data\\finance\\BKKbank\\BKKBankFarmAccount.csv', index_col='datex')
        bkk.index = pd.to_datetime(bkk.index, format="%Y-%m-%d")
        self.bkk1 = bkk.iloc[:, :12]

        self.startdate = pd.to_datetime("2025-01-01")
        self.stopdate = pd.to_datetime("2025-07-01")
        self.idx = pd.date_range(self.startdate, self.stopdate, freq='D')

        self.cost_df = self.create_cost_df()
        self.cost_xfeed_pivot = self.create_cost_xfeed_pivot()
        self.feedcost_pivot = self.create_feedcost_pivot()
        self.create_write_to_csv()
        
        
    def create_cost_df(self):
        
        bkk2 = self.bkk1[   
            (
                (self.bkk1['year'] >= 2025 )
                & (self.bkk1['month'] <7)
                & (self.bkk1['capex'].isnull() )
            )  ].copy()
        
        bkk2  = bkk2.set_index(['year', 'month'])
        bkk2  = bkk2.reset_index()
        
        
        bkk3 = bkk2.drop(columns=['day','transaction', 'index',
            'credit','descr 3','capex'])
        
        bkk3  = bkk3[(bkk3['descr 1'].notna()
                     & (bkk3['descr 1'] != 'nonfarm')
                     & (bkk3['descr 1'] != '?')
                     )
                     ].copy()
        
        
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
        
        bkk5 = bkk4.drop(columns=[ 'feed']) 
        total_row = bkk5.sum(axis=0, numeric_only=True).to_dict() 
        #dict avoids dtype probs in concat with numeric and text in same row
        
        total_row['year'] = 'Total'
        total_row['month'] = ''
       

        bkk5a = pd.concat([bkk5, pd.DataFrame([total_row])], ignore_index=True)
        bkk5a["sum"] = bkk5a.sum(axis=1, numeric_only=True)

        
          

        self.cost_xfeed_pivot = bkk5a
        
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
        bkk9 = bkk9.reset_index()  # Make 'year' and 'month' columns
        # Sum only numeric columns
        numeric_cols = bkk9.select_dtypes(include='number').columns
        total_row = bkk9[numeric_cols].sum(axis=0)
        # Create a dict for the total row with all columns
        total_dict = {col: '' for col in bkk9.columns}
        total_dict.update(total_row)
        total_dict['year'] = 'Total'
        total_dict['month'] = ''
        total_row_df = pd.DataFrame([total_dict], columns=bkk9.columns)

        bkk10 = pd.concat([bkk9, total_row_df], ignore_index=True)
        numeric_cols = bkk10.columns.drop(['year', 'month'])
        bkk10["sum"] = bkk10[numeric_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1)

        self.feedcost_pivot = bkk10
        return self.feedcost_pivot
    
    def create_write_to_csv(self):
        
        self.feedcost_pivot     .to_csv('F:\\COWS\\data\\finance\\BKKbank\\feedcost_pivot.csv')
        self.cost_xfeed_pivot   .to_csv('F:\\COWS\\data\\finance\\BKKbank\\cost_xfeed_pivot.csv')
        
    
if __name__ == "__main__":
    obj=FinanceBasics()
    obj.load_and_process() 