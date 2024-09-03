'''RunInsemUltra.py'''

import pandas as pd
from InsemUltraBasics import InsemUltraBasics
from InsemUltraFunctions import InsemUltraFunctions
from status_2 import StatusData


class Allx:
    def __init__(self):
        
        self.iub = InsemUltraBasics()
        self.iuf = InsemUltraFunctions()
        self.sd  = StatusData()
        
        self.allx = self.create_allx()
        self.write_to_csv()

    def create_allx(self):
        lc = self.iub.last_calf
        tdy = pd.Timestamp.today()
        lastcalf_age = [(tdy - date).days for date in lc['last calf bdate']]
        
        self.iuf.df3['last calf age'] = lastcalf_age
        
        self.iuf.df3_reset = self.iuf.df3.reset_index()
        df4 = self.iuf.df3_reset.merge(self.sd.status_col[['ids','status']], how='left', left_on='WY_id', right_on='ids', suffixes=('', '_right'))
        df4.drop(columns=['ids'], inplace=True)
        df4.set_index('WY_id', inplace=True)
        
        df5 = df4.loc[df4['status'] != 'G']
        
        self.allx = df5
        
        return self.allx
    
    def write_to_csv(self):
    
        self.allx     .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
        self.iuf.ipiv .to_csv ('F:\\COWS\\data\\insem_data\\ipiv.csv')
        
Allx()
     