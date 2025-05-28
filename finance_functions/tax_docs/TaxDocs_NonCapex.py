'''finance_functions/tax_docs/TaxDocs_NonCapex.py'''
import pandas as pd 

class TaxDocs_NonCapex:
    def __init__(self):
        
        bkk1a = pd.read_csv("F:\\COWS\\data\\finance\\BKKbank\\BKKBankFarmAccount.csv")
        bkk1a['datex'] = pd.to_datetime(bkk1a['datex'], errors='coerce')
        bkk1a.set_index(['datex', 'year', 'month'], inplace=True)

        bkk1b= bkk1a.loc[bkk1a.index.get_level_values('year') >= 2024 ]
        bkk1c= bkk1b.iloc[:,:9].copy()
        bkk1c['debit'] = bkk1c['debit'].fillna(0)
        bkk1c['debit'] = bkk1c['debit'].astype(float)
        self.bkk = bkk1c
        
        self.energy_pivot_monthly   = self.createEnergyDoc()
        self.labor_pivot_monthly    = self.createLaborDoc()
        self.medical_pivot_monthly  = self.createMedicalDoc()
        self.maintenance_pivot_monthly = self.createMaintenanceDoc()
        
        self.createFeedDocs()
        
        
    def createFeedDocs(self):
        bkk = self.bkk.copy()
        feed = bkk.loc[(bkk['descr 1'] == 'feed')].copy()
        
        corn = feed.loc[(feed['descr 2'] == 'corn')]
        tapioca = feed.loc[(feed['descr 2'] == 'tapioca')]
        yellow_beans = feed.loc[(feed['descr 2'] == 'yellow beans')]
        
        corn.to_csv('F:\\COWS\\data\\finance\\tax_docs\\corn_2024_H2.csv')
        tapioca.to_csv('F:\\COWS\\data\\finance\\tax_docs\\tapioca_2024_H2.csv')
        yellow_beans.to_csv('F:\\COWS\\data\\finance\\tax_docs\\yellow_beans_2024_H2.csv')

        return
    
    
    def createEnergyDoc(self):
        bkk1 = self.bkk

        bkk2 =bkk1.drop(columns={'day','transaction', 'credit','descr 3','capex','brahman'})

        energy = bkk2.loc[(bkk2['descr 1'] == 'fuel')].copy()
        
        self.energy_pivot_monthly = pd.pivot_table(energy,
                            index=('year','month'),
                              columns ='descr 2',
                              values  ='debit',
                              aggfunc = 'sum'
                              )
                                 
        self.energy_pivot_monthly.to_csv('F:\\COWS\\data\\finance\\tax_docs\\energy_pivot_monthly.csv')
        
        return  self.energy_pivot_monthly
        
        
    def createLaborDoc(self):
        bkk1 = self.bkk

        bkk2 =bkk1.drop(columns={'day','transaction', 'credit','descr 3','brahman'})

        labor1 = bkk2.loc[(
           ( bkk2['descr 1'] == 'labor') & (bkk2['capex'] != 'x'))
                         ].copy()
        
        labor = labor1.drop(columns={'capex'})
        
        self.labor_pivot_monthly = pd.pivot_table(labor,
                            index=('year','month'),
                              columns ='descr 2',
                              values  ='debit',
                              aggfunc = 'sum'
                              )
                                 
        self.labor_pivot_monthly.to_csv('F:\\COWS\\data\\finance\\tax_docs\\labor_pivot_monthly.csv')
        
        return  self.labor_pivot_monthly
    
    
    def createMedicalDoc(self):
        bkk1 = self.bkk

        bkk2 =bkk1.drop(columns={'day','transaction', 'credit','descr 3','brahman', 'capex'})

        medical = bkk2.loc[( bkk2['descr 1'] == 'medical')] .copy()
        
        self.medical_pivot_monthly = pd.pivot_table(medical,
                            index=('year','month'),
                              columns ='descr 2',
                              values  ='debit',
                              aggfunc = 'sum'
                              )
            

        self.medical_pivot_monthly['sum'] = self.medical_pivot_monthly.sum(axis=1)

        col_sum = self.medical_pivot_monthly.sum(axis=0)
        self.medical_pivot_monthly.loc[('total', ""), :] = col_sum


        self.medical_pivot_monthly.to_csv('F:\\COWS\\data\\finance\\tax_docs\\medical_pivot_monthly.csv')
        
        return  self.medical_pivot_monthly
             
    def createMaintenanceDoc(self):
        bkk1 = self.bkk

        bkk2 =bkk1.drop(columns={'day','transaction', 'credit','descr 3','brahman'})

        maintenance1 = bkk2.loc[(
           ( bkk2['descr 1'] == 'maintenance') & (bkk2['capex'] != 'x'))
                         ].copy()
        
        maintenance = maintenance1.drop(columns={'capex'})
        
        self.maintenance_pivot_monthly = pd.pivot_table(maintenance,
                            index=('year','month'),
                              columns ='descr 2',
                              values  ='debit',
                              aggfunc = 'sum'
                              )
        
        self.maintenance_pivot_monthly['sum'] = self.maintenance_pivot_monthly.sum(axis=1)
        col_sum = self.maintenance_pivot_monthly.sum(axis=0)
        self.maintenance_pivot_monthly.loc[('total', ""), :] = col_sum
                                 
        self.maintenance_pivot_monthly.to_csv('F:\\COWS\\data\\finance\\tax_docs\\maintenance_pivot_monthly.csv')
        
        return  self.maintenance_pivot_monthly
    
        
if __name__ == "__main__":
    TaxDocs_NonCapex()
()        
