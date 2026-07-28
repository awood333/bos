'''insem_functions/ipiv_pivot_table.py'''

import inspect
import pandas as pd
from sql_db_related.neon_connect import get_engine
from container import get_dependency


class IpivPivotTable:
    def __init__(self):
        print(f"ipiv_pivot_table instantiated by: {inspect.stack()[1].filename}")
        
        self.IUD = None
        self.ipiv_milkers =None
        self.ipiv_data = None
        self.ipiv_pivot_table = None
        self.pt=None
    
    def load(self):
         
        self.IUD = get_dependency('insem_ultra_data')
        self.process()
        
    def process(self):
        
        engine = get_engine()
        with engine.connect() as conn:
            self.ipiv_data = pd.read_sql_table('ipiv_data_formatted', conn)        
        
        #methods
        self.ipiv_pivot_table = self.create_ipiv_pivot_table()
        self.pt = self.join_cols_to_pivot()

    
    
    def create_ipiv_pivot_table(self):
        df_1 = self.ipiv_data
        df_2 = df_1.drop(columns=['try_num']) 
        self.pt = pd.pivot_table(df_2,
                            index= 'wy_id',
                            columns= 'lact_num',
                            values= 'insem_date')
        return self.pt
        
    def join_cols_to_pivot(self):
        

        xxx = self.IUD.allx[['wy_id', 'u_read', 'days_milking']].set_index('wy_id', drop=True)
        xxx.index = xxx.index.astype(int)#.astype(str)        
        merge_1 = pd.merge(xxx, self.pt, how='right', left_index=True, right_index=True)
        merge_1.index = merge_1.index.astype(int)

        self.ipiv_pivot_table = merge_1.reset_index().sort_values('wy_id').reset_index(drop=True)
        return self.ipiv_pivot_table
    

if __name__ == "__main__":
    obj=IpivPivotTable()
    obj.load()
        