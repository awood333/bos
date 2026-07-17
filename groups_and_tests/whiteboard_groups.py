'''milk_functions.WhiteboardGroups.py'''


import inspect

import pandas as pd
from sql_db_related.neon_connect import get_engine



class WhiteboardGroups:
    def __init__(self):

        print(f"WhiteboardGroups instantiated by: {inspect.stack()[1].filename}")
        self.whiteboard_groups_tenday = self.neon_data_loader()

    def neon_data_loader(self):
        engine = get_engine()
        with engine.connect() as conn:
           
            wbg1 = pd.read_sql("SELECT * FROM latest_groups_payload_view;", conn)
            date = str(wbg1['snapshot_date'][0])     
            wbgroups2 = wbg1.drop(columns=['snapshot_date'])       
            wbgroups2 = wbgroups2.rename(columns= {'group_name': date, 'wy_id' : 'wy_id'})
            self.whiteboard_groups_tenday = wbgroups2
        
        
        return self.whiteboard_groups_tenday


if __name__ == "__main__":
    obj = WhiteboardGroups()
