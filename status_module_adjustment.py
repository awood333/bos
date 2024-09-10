'''status_module_adjustment.py'''

import pandas as pd
from datetime import datetime
from status_2 import StatusData
from insem_ultra import InsemUltraData

today = pd.to_datetime(datetime.today())
class StatusModuleAdjustment:
    def __init__(self):

        self.sd         = StatusData()
        self.iud        = InsemUltraData()       

#  functions
        self.allx = self.create_allx()
        # self.create_write_to_csv()

    # def create_all2(self):
        
    #     status_col = self.sd.status_col
    #     all1_reset = self.iud.all1.reset_index()
        
    #     all2 = all1_reset.merge(status_col[['ids','status']], how='left', left_index=True, right_on='ids', suffixes=('', '_right'))
    #     all2.drop(columns=['ids'], inplace=True)
    #     all2.set_index('WY_id', inplace=True)
        
    #     all2.to_csv('F:\\COWS\\data\\insem_data\\all.csv')
    #     return all2

    # def create_write_to_csv(self):
    #     self.all2.to_csv('F:\\COWS\\data\\insem_data\\all.csv')