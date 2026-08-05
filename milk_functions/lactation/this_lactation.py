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
                
        self.last_calf_bdate= None
        self.last_calf_num  = None
        self.alive_ids      = None
        self.period_df      = None
        self.period_weekly  = None
        self.this_lact_wkly = None
        self.this_lact_daily = None

    def load(self):
        self.MB  = get_dependency('milk_basics')
        self.WD  = get_dependency('wet_dry')
        self.SD  = get_dependency('status_data')
        self.IUB = get_dependency('insem_ultra_basics')
        self.process()
        
    def process(self):
        
        self.milk           = self.MB.data['milk']
        self.last_calf_bdate= self.IUB.last_calf['last_calf_bdate']
        self.last_calf_num  = self.IUB.last_calf['last_calf_num']
        self.alive_ids      = self.SD .alive_ids_today

        
        #methods
        self.this_lact_daily,  = self.create_this_lactation_daily()
        

    def create_this_lactation_daily(self):
        lcbdate = self.last_calf_bdate

  
        
        return self.this_lact_daily
    

   
if __name__ == "__main__":
    obj = ThisLactation()
    obj.load()      