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
        # lc = self.iub.last_calf
        # tdy = pd.Timestamp.today()
        # lastcalf_age = [(tdy - date).days for date in lc['last calf bdate']]
        
        # self.iuf.df5['last calf age'] = lastcalf_age
        
        self.iuf.df5_reset = self.iuf.df5.reset_index()
        df6 = self.iuf.df5_reset.merge(self.sd.status_col[['ids','status']], 
                                       how='left', 
                                       left_on='WY_id', 
                                       right_on='ids', 
                                       suffixes=('', '_right'))
        
        df6.drop(columns=['ids'], inplace=True)
        df6.set_index('WY_id', inplace=True)
        
        df7 = df6.loc[df6['status'] != 'G']
        new_col_order =  [
            'status',
            'last stop date',
            'stop calf#',
            'last calf bdate',
            'last calf#',
            'last calf age',
            # 'days milking',
            'i_calf#',
            'i_date',
            'age insem',
            'u_calf#',
            'u_date',
            'u_read',
            'age ultra',
            'expected bdate'
            ]
        df7 = df7[new_col_order]
        
        self.allx = df7
        
        return self.allx
    
    def write_to_csv(self):
        
        # print('allx ', self.allx.iloc[:5,:])
    
        self.allx     .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
        self.iuf.ipiv .to_csv ('F:\\COWS\\data\\insem_data\\ipiv.csv')
        
Allx()
     