'''finance/BKK_bank.py'''

import pandas as pd 
from datetime import datetime
tdy = datetime.now()

class BKK_bank:
    def __init__(self):
        self.bkk = self.load_data()
        self.bkk2 = self.partition_data()
        self.bkk_groups_pivot = self.group_data()
        self.bkkbank_dash_vars = self.get_dash_vars()
        self.write_to_csv()
        
    
    def load_data(self):
        file_path = 'F:\\COWS\\data\\finance\\BKKbank\\BKKbank_consol_statement.ods'
        self.bkk = pd.read_excel(file_path, engine='odf')
        return self.bkk
    
    def partition_data(self):
        bkk1 = self.bkk.loc[
            (self.bkk['year'] == 2024)]
        
        self.bkk2 = bkk1[['year','month','debit','descr 1','descr 2']]
        return self.bkk2
    
    def group_data(self):
        bkk_groups_by_month = self.bkk2.groupby(['month','descr 1']).agg(
            {'debit'    : 'sum'}
        ).reset_index()
        
        self.bkk_groups_pivot = pd.pivot_table(bkk_groups_by_month, 
                                    index   = 'month',
                                    columns = 'descr 1',
                                    values  = 'debit',
                                    aggfunc = 'sum'
                                    )
        
        return self.bkk_groups_pivot
        
    
    def get_dash_vars(self):
        self.bkkbank_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.bkkbank_dash_vars
    
    
    def write_to_csv(self):
        self.bkk_groups_pivot.to_csv("F:\\COWS\\data\\finance\\bkk_groups_pivot.csv") 
    
if __name__ == "__main__":
    bank = BKK_bank()