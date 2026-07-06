'''milk_functions.lactations.this_lactation.py'''

import inspect
from container import get_dependency
from pathlib import Path

class ThisLactation():
    def __init__(self):
        print(f"ThisLactation instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.WD = None
        # self.SD = None
        self.milking_wkly = None
        self.milking_daily = None

    def load_and_process(self):

        self.MB = get_dependency('milk_basics')
        self.WD = get_dependency('wet_dry')
        # self.SD = get_dependency('status_data')
        self.milking_wkly, self.milking_daily,  = self.create_daily_weekly()
                

    def create_daily_weekly(self):
        milking_daily = self.WD.milking_liters 
        # milking_liters is an ongoing lact (in WD)-- there can only be one....
        grouping_key  = milking_daily.index //7 +1
        self.milking_wkly  = milking_daily.groupby(grouping_key).mean()
        self.milking_daily = milking_daily
        return self.milking_wkly, self.milking_daily
    

   
if __name__ == "__main__":
    obj = ThisLactation()
    obj.load_and_process()      