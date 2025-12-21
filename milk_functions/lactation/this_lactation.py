'''milk_functions.lactations.this_lactation.py'''

import inspect
from container import get_dependency

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
        # self.SD = get_dependency('statusData')
        self.milking_wkly, self.milking_daily,  = self.create_daily_weekly()
        
        self.create_write_to_csv()
        

    def create_daily_weekly(self):
        milking_daily = self.WD.milking_liters 
        # milking_liters is an ongoing lact (in WD)-- there can only be one....
        grouping_key  = milking_daily.index //7 +1
        self.milking_wkly  = milking_daily.groupby(grouping_key).mean()
        self.milking_daily = milking_daily
        return self.milking_wkly, self.milking_daily
    

    
    def create_write_to_csv(self):
        self.milking_wkly.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\this_Lact_weekly.csv')
        self.milking_daily.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\this_Lact_daily.csv')
        
if __name__ == "__main__":
    obj = ThisLactation()
    obj.load_and_process()      