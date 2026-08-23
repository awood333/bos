'''milk_functions/milk_aggregates_basic.py'''

import inspect
import pandas as pd
import numpy as np
from container import get_dependency
from sql_db_related.neon_connect import get_engine

class DailyMilkVsFullday:
    def __init__(self):
        print(f"MilkAggregatesBasic instantiated by: {inspect.stack()[1].filename}")
        self.engine = get_engine()

    def load(self):
        self.MA = get_dependency('milk_aggregates_basic')        
        self.process()
        
    def process(self):
        
        start = pd.to_datetime("2025-09-20" )
        fullday_1 = self.MA.fullday.loc[start :, :]
        self.fullday = fullday_1.sum(axis=1).rename('fullday').to_frame()
                
        df = self._read_neon_query()
        df['sale + heldback'] = df['sale_total'] + df['heldback_total']
        self.sale = df

        self.fullday_sale_diff = self.compare_fullday_sale()
        
    def _read_neon_query(self):

        with self.engine.connect() as conn:
            return pd.read_sql_table('daily_milk', conn)
        
    def compare_fullday_sale(self):
        ''' sale is from the CP receipts, fullday is from our whiteboard'''
        diff_1 = pd.merge(self.fullday,self.sale,
                                  on='datex',
                                  how='outer')
        
        diff_1['diff'] = (diff_1['fullday'] - diff_1['sale + heldback'])
        
        self.fullday_sale_diff = diff_1
        return self.fullday_sale_diff

        
        
        
        
    
            
                
                        
                        
                        
                        
if __name__ == '__main__':
    obj = DailyMilkVsFullday()
    obj.load()                        