'''milk_functions.lactations.this_lactation.py'''

import inspect
from container import get_dependency

class ThisLactation():
    def __init__(self):
        print(f"ThisLactation instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.WD = None
        self.IUD = None
        self.SD = None
        # self.SD = None
        self.this_lact_wkly = None
        self.this_lact_daily = None

    def load(self):
        self.MB  = get_dependency('milk_basics')
        self.WD  = get_dependency('wet_dry')
        self.SD  = get_dependency('alive_ids_today')
        self.IUD = get_dependency('insem_ultra_data')
        self.process()
        
    def process(self):
        
        self.last_calf_bdate= self.IUD.last_calf_bdate
        self.last_calf_num  = self.IUD.last_calf_num
        self.alive_ids      = self.SD .alive_ids_today
        self.period_df      = self.WD .period_df
        self.period_weekly  = self.WD .period_weekly
        
        #methods
        self.this_lact_wkly, self.this_lact_daily,  = self.create_daily_weekly()
        

    def create_daily_weekly(self):
        # this_lact_daily = self.WD.this_lact_liters 
        # # this_lact_liters is an ongoing lact (in WD)-- there can only be one....
        # grouping_key  = this_lact_daily.index //7 +1
        # self.this_lact_wkly  = this_lact_daily.groupby(grouping_key).mean()
        # self.this_lact_daily = this_lact_daily
        return self.this_lact_wkly, self.this_lact_daily
    

   
if __name__ == "__main__":
    obj = ThisLactation()
    obj.load()      