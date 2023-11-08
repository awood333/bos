'''
status_module_adjustment.py
'''

import pandas as pd
from status import StatusData
from insem_ultra import InsemUltraData


class StatusModuleAdjustment:
    def __init__(self):
        
        self.sd         = StatusData()
        self.iud        = InsemUltraData()
        
        # sd.create_write_to_csv()
        # iud.create_write_to_csv()
        
        #  functions
        self.all        = self.merge_all1_with_status_col()
        self.create_write_to_csv()
    
    def merge_all1_with_status_col(self):
        
        all = self.iud.all1.merge(self.sd.status_col,  how='left', on="WY_id", suffixes=('', "_right") )
        # all.drop([i for i in all.columns if '_right' in i], axis=1, inplace=True)
        
        return all

    
    def create_write_to_csv(self):
        self.all.to_csv('F:\\COWS\\data\\insem_data\\all.csv')
        
# sma = StatusModuleAdjustment()
# sma.merge_all1_with_status_col
    
        