'''finance_functions/tax_docs/tax_docs_depreciation.py'''

import inspect
import pandas as pd 

class TaxDocs_Depreciation:
    def __init__(self):
        
        print(f"TaxDocs_Depreciation instantiated by: {inspect.stack()[1].filename}")
        
        bkk1a = pd.read_csv("F:\\COWS\\data\\finance\\BKKbank\\BKKBankFarmAccount.csv")
        bkk1a['datex'] = pd.to_datetime(bkk1a['datex'], errors='coerce')
        bkk1a.set_index(['datex', 'year', 'month'], inplace=True)

        bkk1b= bkk1a.loc[(
            (bkk1a.index.get_level_values('year') >= 2024) 
            & bkk1a['credit'].isnull()
             )].copy()
        
        bkk1c= bkk1b.iloc[:,:12].copy()
        bkk1c['debit'] = bkk1c['debit'].fillna(0)
        bkk1c['debit'] = bkk1c['debit'].astype(float)
        self.bkk = bkk1c
        
        bkk5 = self.bkk.loc[self.bkk['capex'] == 'x'].copy()
        self.capex = bkk5.drop(columns={'day','transaction', 'brahman'})
        
        
        [self.depreciationPivot_monthly, 
         self.depreciationPivot_annual]     = self.createDepreciationDoc()
        
        
        
    def createDepreciationDoc(self):
        
        bkk6 = self.capex
        
        bkk7_monthly = bkk6.groupby(['year', 'month', 'descr 1' ]).agg(
            {'debit' : 'sum'}            
            )
        
        bkk7_annual = bkk6.groupby(['year',  'descr 1' ]).agg(
            {'debit' : 'sum'}            
            )

        bkk8_monthly = pd.pivot_table( bkk7_monthly,
                              index   =('year','month'),
                              columns ='descr 1',
                              values  ='debit')
        
        bkk8_monthly['sum'] = bkk8_monthly.sum(axis=1)
        
        bkk8_annual = pd.pivot_table( bkk7_annual,
                              index   =('year'),
                              columns ='descr 1',
                              values  ='debit')
                
        bkk8_annual['sum'] = bkk8_annual.sum(axis=1)

        self.depreciationPivot_monthly = bkk8_monthly        
        self.depreciationPivot_annual  = bkk8_annual
 
        self.depreciationPivot_monthly  .to_csv('F:\\COWS\\data\\finance\\tax_docs\\depreciation_pivot_monthly.csv')
        self.depreciationPivot_annual   .to_csv('F:\\COWS\\data\\finance\\tax_docs\\depreciation_pivot_annual.csv')
        
        return self.depreciationPivot_monthly, self.depreciationPivot_annual
    
    
if __name__ == "__main__":
    obj = TaxDocs_Depreciation()
    obj.load_and_process()    