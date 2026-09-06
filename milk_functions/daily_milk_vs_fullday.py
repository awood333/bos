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
        self.fullday = wy_total.sum(axis=1).rename('wy').to_frame()
        self.daily_milk, self.wy = self._read_neon_query()
        self.daily_milk_vs_fullday = self.compare_wy_cp()
        self.write_to_csv()
        
    def _read_neon_query(self):
        #this is 'daily_milk' table in Neon
        with self.engine.connect() as conn:
            daily_milk_df_1 = pd.read_sql_table('daily_milk', conn)
            daily_milk_df_2 = daily_milk_df_1.iloc[ -10 : , :].copy()
            self.daily_milk = daily_milk_df_2.rename(columns={'sale_total' : 'cp'})
            
            milk_totals_df_1 = pd.read_sql_table('milk_totals', conn)
            milk_totals_df_2 = milk_totals_df_1.iloc[-10:, :][['datex', 'total_liters']].copy()
            self.wy = milk_totals_df_2.rename(columns={'total_liters': 'wy'})
        return self.daily_milk, self.wy
        
    def compare_wy_cp(self):
        ''' cp is from the cp receipts, wy_total is from our whiteboard'''
        diff_1 = pd.merge(self.fullday,self.daily_milk,
                                  on='datex',
                                  how='outer')
        diff_1['wy_x_heldback'] = diff_1['wy'] - diff_1['heldback_total']
        diff_1['wy_minus_cp'] = (diff_1['wy_x_heldback'] - diff_1['cp'])

        
        self.daily_milk_vs_fullday = diff_1
        return self.daily_milk_vs_fullday

        
    def write_to_csv(self):
        output_dir = Path("/home/alanw/Documents/vsCode_output/milk")
        output_dir.mkdir(parents=True, exist_ok=True)
        self.daily_milk_vs_fullday.to_csv(output_dir / "daily_milk_vs_fullday.csv")   
        
        
        
        
    
            
                
                        
                        
                        
                        
if __name__ == '__main__':
    obj = DailyMilkVsFullday()
    obj.load()                        