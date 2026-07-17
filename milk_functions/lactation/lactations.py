'''Lactations.py'''
import inspect
# from pathlib import Path
import pandas as pd 
# import numpy as np
from container import get_dependency


class Lactations:
    def __init__(self):
        print(f"Lactations instantiated by: {inspect.stack()[1].filename}")
        self.Lacts = None
        self.MB = None
        self.SD = None
        self.wy_id_list = None
        self.alive_ids = None
        self.L1 = self.L2 = self.L3 = self.L4 = self.L5 = self.L6 = None
        self.live_L1 = self.live_L2 = self.live_L3 = self.live_L4 = self.live_L5 = self.live_L6 = None

    def load(self):

        self.Lacts  = get_dependency('lactation_basics')
        self.MB     = get_dependency('milk_basics')
        self.SD     = get_dependency('status_data')
        self.process()
        
    def process(self):

        self.headers = self.Lacts.headers
        self.alive_ids_int  = self.SD.alive_ids_last
        self.alive_ids_str  = [str(i) for i in self.SD.alive_ids_last]
        self.create_separate_lactations()
        self.create_live_lactations()
        
    
    def create_separate_lactations(self):

        headers = self.headers
        
        L = self.Lacts.lactations_array
        print('L.shape', L.shape)
        
        #3rd dimension is the lactations dim
        self.L1 = pd.DataFrame(L[:,:,0], columns=headers)
        self.L2 = pd.DataFrame(L[:,:,1], columns=headers)
        self.L3 = pd.DataFrame(L[:,:,2], columns=headers)
        self.L4 = pd.DataFrame(L[:,:,3], columns=headers)
        self.L5 = pd.DataFrame(L[:,:,4], columns=headers)
        self.L6 = pd.DataFrame(L[:,:,5], columns=headers)
       
        return [self.L1,self.L2,self.L3,
                self.L4,self.L5,self.L6]
    

    def create_live_lactations(self):
        self.live_L1 = self.L1.loc[:,self.alive_ids_str]
        self.live_L2 = self.L2.loc[:,self.alive_ids_str]
        self.live_L3 = self.L3.loc[:,self.alive_ids_str]
        self.live_L4 = self.L4.loc[:,self.alive_ids_str]
        self.live_L5 = self.L5.loc[:,self.alive_ids_str]
        self.live_L6 = self.L6.loc[:,self.alive_ids_str]

        return [self.live_L1, self.live_L2 , self.live_L3,
                   self.live_L4, self.live_L5, self.live_L6]

if __name__ == "__main__":
    obj=Lactations()
    obj.load()     