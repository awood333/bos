'''milk_functions\\model_groups.py'''
import inspect
import pandas as pd
from container import get_dependency

class ModelGroups:

    def __init__(self):

        print(f"ModelGroups instantiated by: {inspect.stack()[1].filename}")

        self.SD = None
        self.WD = None
        self.IUB = None
        self.IUD = None
        self.MB = None
        self.DR = None
        self.MA = None
        
        #process
       
        
        #methods
       


    def load(self):

        self.SD = get_dependency('status_data')
        self.WD = get_dependency('wet_dry')
        self.IUD= get_dependency('insem_ultra_data')
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')        
        self.MA = get_dependency('milk_aggregates')
        self.BSO= get_dependency('bos_state_orchestrator')
        self.process()
        
    def process(self):
        self.startdate  = self.DR.startdate
       
        
   
        #methods

        
        
         
if __name__ == "__main__":
    model_groups = ModelGroups()
    model_groups.load()    