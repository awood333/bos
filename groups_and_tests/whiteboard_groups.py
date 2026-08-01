'''milk_functions.WhiteboardGroups.py'''


import inspect
import pandas as pd
from sql_db_related.neon_connect import get_engine
from container import get_dependency


class WhiteboardGroups:
    def __init__(self):

        print(f"WhiteboardGroups instantiated by: {inspect.stack()[1].filename}")
        self.tenday_avg = None
        self.allx = pd.DataFrame()
        self.whiteboard_groups_tenday = pd.DataFrame()
        
        
        
    def load_and_process(self):
        IUD = get_dependency('insem_ultra_data')
        MA  = get_dependency('milk_aggregates')
        
        self.allx = IUD.allx
        tenday_avg_1 = MA.tenday.loc[:,['wy_id','avg']]
        self.tenday_avg = tenday_avg_1.set_index('wy_id')
        
        # methods
        self.whiteboard_groups_tenday = self.neon_data_loader()



    def neon_data_loader(self):
        
        engine = get_engine()
        with engine.connect() as conn:
           
            wbg1 = pd.read_sql("SELECT * FROM latest_groups_payload_view;", conn)
            wbgroups2 = wbg1.set_index('wy_id')
            
              
            wbgroups4 = wbgroups2.merge(self.tenday_avg,
                        how='left', 
                        left_index=True, 
                        right_index=True
                        )            
            
            
            days1 = pd.DataFrame(self.allx.loc[:,['wy_id','days_milking', 'u_read', 'expected_bdate']])
            days = days1.set_index('wy_id')
            
            wbgroups5 = wbgroups4.merge(days, 
                        how='left', 
                        left_index=True, 
                        right_index=True
                        )
            
            wbgroups6 = wbgroups5.sort_values('avg', ascending=False) #sort on the avg col
            wbgroups6['snapshot_date'] = wbgroups6.pop('snapshot_date') #move snapshot to last col
            wbgroups6 = wbgroups6.reset_index(drop=False) #keep wy_id as col
            
            self.whiteboard_groups_tenday = wbgroups6

        
        return self.whiteboard_groups_tenday


if __name__ == "__main__":
    obj = WhiteboardGroups()
    obj.load_and_process()
