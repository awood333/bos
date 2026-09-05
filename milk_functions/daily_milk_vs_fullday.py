'''milk_functions/milk_aggregates_basic.py'''

import inspect
import pandas as pd
from   pathlib import Path
from container import get_dependency
from sql_db_related.neon_connect import get_engine

class DailyMilkVsFullday:
    def __init__(self):
        print(f"MilkAggregatesBasic instantiated by: {inspect.stack()[1].filename}")
        self.engine = get_engine()
        self.daily_milk = pd.DataFrame()

    def load(self):
        self.MA = get_dependency('milk_aggregates_basic')        
        self.process()
        
    def process(self):
        
        wy_total = self.MA.fullday.iloc[ -10 :, :]
        self.fullday = wy_total.sum(axis=1).rename('WY').to_frame()
        self.daily_milk, self.WY = self._read_neon_query()
        self.WY_CP_diff = self.compare_WY_CP()
        self.write_to_csv()
        
    def _read_neon_query(self):
        #this is 'daily_milk' table in Neon
        with self.engine.connect() as conn:
            daily_milk_df_1 = pd.read_sql_table('daily_milk', conn)
            daily_milk_df_2 = daily_milk_df_1.iloc[ -10 : , :].copy()
            self.daily_milk = daily_milk_df_2.rename(columns={'sale_total' : 'CP'})
            
            milk_totals_df_1 = pd.read_sql_table('milk_totals', conn)
            milk_totals_df_2 = milk_totals_df_1.iloc[-10:, :][['datex', 'total_liters']].copy()
            self.WY = milk_totals_df_2.rename(columns={'total_liters': 'WY'})
        return self.daily_milk, self.WY
        
    def compare_WY_CP(self):
        ''' CP is from the CP receipts, WY_total is from our whiteboard'''
        diff_1 = pd.merge(self.fullday,self.daily_milk,
                                  on='datex',
                                  how='outer')
        diff_1['WY - heldback'] = diff_1['WY'] - diff_1['heldback_total']
        diff_1['WY-CP'] = (diff_1['WY'] - diff_1['WY - heldback'])

        
        self.WY_CP_diff = diff_1
        return self.WY_CP_diff

        
    def write_to_csv(self):
        output_dir = Path("/home/alanw/Documents/vsCode_output/milk")
        output_dir.mkdir(parents=True, exist_ok=True)
        self.WY_CP_diff.to_csv(output_dir / "WY_CP_diff.csv")   
        
        
        
        
    
            
                
                        
                        
                        
                        
if __name__ == '__main__':
    obj = DailyMilkVsFullday()
    obj.load()                        