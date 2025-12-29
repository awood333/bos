'''Lactations.py'''
import inspect
import pandas as pd 
from container import get_dependency


class Lactations:
    def __init__(self):
        print(f"Lactations instantiated by: {inspect.stack()[1].filename}")
        self.Lacts = None
        self.MB = None
        self.SD = None
        self.WY_ids = None
        self.alive_ids = None
        self.L1 = self.L2 = self.L3 = self.L4 = self.L5 = self.L6 = None
        self.live_L1 = self.live_L2 = self.live_L3 = self.live_L4 = self.live_L5 = self.live_L6 = None

    def load_and_process(self):

        self.Lacts  = get_dependency('lactation_basics')
        self.MB     = get_dependency('milk_basics')
        self.SD     = get_dependency('status_data')
        self.WY_ids = self.MB.data['WY_ids']

        self.alive_ids = self.SD.alive_ids.astype(int).to_list()
        [self.L1, self.L2, self.L3, 
         self.L4, self.L5, self.L6]          = self.create_separate_lactations()
        
        [self.live_L1, self.live_L2, self.live_L3, 
         self.live_L4, self.live_L5, self.live_L6] = self.create_live_lactations()
        
        self.write_to_csv()
    
    def create_separate_lactations(self):

        L = self.Lacts.lactations_array
        self.L1 = pd.DataFrame(L[0], columns=self.WY_ids)
        self.L2 = pd.DataFrame(L[1], columns=self.WY_ids)
        self.L3 = pd.DataFrame(L[2], columns=self.WY_ids)
        self.L4 = pd.DataFrame(L[3], columns=self.WY_ids)
        self.L5 = pd.DataFrame(L[4], columns=self.WY_ids)
        self.L6 = pd.DataFrame(L[5], columns=self.WY_ids)
       
        return [self.L1,self.L2,self.L3,
                self.L4,self.L5,self.L6]
    

    def create_live_lactations(self):
        self.live_L1 = self.L1.loc[:,self.alive_ids]
        self.live_L2 = self.L2.loc[:,self.alive_ids]
        self.live_L3 = self.L3.loc[:,self.alive_ids]
        self.live_L4 = self.L4.loc[:,self.alive_ids]
        self.live_L5 = self.L5.loc[:,self.alive_ids]
        self.live_L6 = self.L6.loc[:,self.alive_ids]

        return [self.live_L1, self.live_L2 , self.live_L3,
                   self.live_L4, self.live_L5, self.live_L6]
        
    def write_to_csv(self):

        self.live_L1.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_1.csv')
        self.live_L2.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_2.csv')
        self.live_L3.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_3.csv')
        self.live_L4.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_4.csv')
        self.live_L5.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_5.csv')
        self.live_L6.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_6.csv')


    
if __name__ == "__main__":
    obj=Lactations()
    obj.load_and_process()     