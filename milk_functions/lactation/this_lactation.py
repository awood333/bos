'''milk_functions.lactations.lactation_this.py'''

import inspect
from milk_basics import MilkBasics
from container import get_dependency
from persistent_container_service import ContainerClient

class ThisLactation():
    def __init__(self):
        print(f"ThisLactation instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.WD = None
        self.milking = None

    def load_and_process(self):
        client = ContainerClient()
        self.MB = MilkBasics()
        self.WD = client.get_dependency('wet_dry')
        self.milking = self.create_weekly()
        self.create_write_to_csv()
        

    def create_weekly(self):
        milking = self.WD.milking_liters # milking_liters is an ongoing lact (in WD)-- there can only be one....
        grouping_key = milking.index //7 
        self.milking = milking.groupby(grouping_key).mean()
        return self.milking
    

    
    def create_write_to_csv(self):
        self.milking.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\milking_thisLact.csv')
        
if __name__ == "__main__":
    ThisLactation()  