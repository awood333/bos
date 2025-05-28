import pandas as pd 
from milk_functions.lactations.lactation_basics import Lactation_basics
from MilkBasics import MilkBasics


class Lactations:
    def __init__(self):
        
        self.Lacts = Lactation_basics()
        self.MB     = MilkBasics()
        self.WY_ids = self.MB.data['WY_ids']
        
        [self.L1,self.L2,self.L3,
        self.L4,self.L5,self.L6] = self.create_separate_lactations()
    
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
        
        
    def write_to_csv(self):
        self.L1.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_1.csv')
        self.L2.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_2.csv')
        self.L3.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_3.csv')
        self.L4.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_4.csv')
        self.L5.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_5.csv')
        self.L6.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daily\\lactation_6.csv')

    
if __name__ == "__main__":
    Lactations()