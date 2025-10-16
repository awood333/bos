'''finance/CapexBasics.py'''

from datetime import datetime
import pandas as pd
from container import get_dependency, container

tdy = datetime.now()
timestamp = tdy.strftime('%Y-%m-%d_%H-%M-%S')

class CapexBasics:
    def __init__(self):
        
        DR = get_dependency('date_range')
        self.start_date = DR.startdate
        
        self.bkk    = self.load_partition_data()
        self.capex_details, self.non_capex_details  = self.create_capex()
        
        self.capex_pivot     = self.group_capex_data()
        self.non_capex_pivot = self.group_non_capex_data()
        self.write_to_csv()
        
    
    def load_partition_data(self):
        bkk1 = pd.read_csv('F:\\COWS\\data\\finance\\BKKbank\\BKKbankFarmAccount.csv')
        bkk1['datex'] = pd.to_datetime(bkk1['datex'])
        bkk1.set_index('datex', inplace=True)
        
        # backup the original
        bkk1    .to_csv(f"F:\\COWS\\data\\finance\\BKKbank\\backup\\BKKbank_{timestamp}.csv")
        bkk1    .to_csv(f"E:\\Cows\\data_backup\\finance_backup\\farm_account\\BKKbank_{timestamp}.csv")

        bkk1a = bkk1.iloc[:,:12].copy()
        bkk1b = bkk1a.loc[(
            ( bkk1['year'] >= 2017))
            & bkk1['credit'].isnull()
                        ].copy() 
        
        bkk1c = bkk1b.drop(columns={'credit'})
        bkk1d = bkk1c.iloc[:,:14].copy()
        
        bkk2  = bkk1d.loc[
            (bkk1b['descr 1'] != 'nonfarm')
            & (bkk1b['descr 1'] != '?')
            ].copy()
        
        bkk2['debit'] = bkk2['debit'].fillna(0)
        bkk2['debit'] = bkk2['debit'].astype(float)
        
        bkk3 = bkk2[['year','month','day','transaction'
                     ,'debit','descr 1','descr 2'
                     , 'descr 3','capex']].copy()
        self.bkk = bkk3
        
        return self.bkk
    
    def create_capex(self):
        self.capex_details = self.bkk.loc[self.bkk['capex'] == 'x']
        self.non_capex_details = self.bkk.loc[self.bkk['capex'] != 'x']
        return self.capex_details, self.non_capex_details
    
    def group_capex_data(self):
        
        capex1 = self.capex_details
        
        capex_by_month = capex1.groupby(['year','month','descr 1']).agg(
            {'debit'    : 'sum'}
        ).reset_index()
        
        capex_pivot1 = pd.pivot_table(capex_by_month,
                                                
                                    index   = ('year','month'),
                                    columns = 'descr 1',
                                    values  = 'debit',
                                    aggfunc = 'sum'
                                    )
        
        capex_pivot1['sum'] = capex_pivot1.sum(axis=1)
        self.capex_pivot = capex_pivot1
        
        return self.capex_pivot
        
              
    def group_non_capex_data(self):
        
        non_capex1 = self.non_capex_details
        non_capex2 = non_capex1.loc[self.start_date:,:]
        
        non_capex_by_month = non_capex2.groupby(['year','month','descr 1']).agg(
            {'debit'    : 'sum'}
        ).reset_index()
        
        non_capex_pivot1 = pd.pivot_table(non_capex_by_month,
                                                
                                    index   = ('year','month'),
                                    columns = 'descr 1',
                                    values  = 'debit',
                                    aggfunc = 'sum'
                                    )
        
        non_capex_pivot2 = non_capex_pivot1.drop(columns=(['feed']))
        non_capex_pivot2['sum'] = non_capex_pivot2.sum(axis=1)
        self.non_capex_pivot = non_capex_pivot2

        return self.non_capex_pivot
        

    def write_to_csv(self):
        
        self.capex_details      .to_csv("F:\\COWS\\data\\finance\\capex\\capex_details.csv")
        self.non_capex_details  .to_csv("F:\\COWS\\data\\finance\\capex\\non_capex_details.csv")

        self.capex_pivot    .to_csv("F:\\COWS\\data\\finance\\capex\\capex_pivot.csv")
        self.non_capex_pivot.to_csv("F:\\COWS\\data\\finance\\capex\\non_capex_pivot.csv") 
 
    
if __name__ == "__main__":
    CapexBasics()