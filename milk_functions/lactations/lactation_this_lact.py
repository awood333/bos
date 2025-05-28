'''milk_functions.lactations.lactation_this.py'''

from MilkBasics import MilkBasics
from status_functions.WetDry import WetDry

class ThisLactation():
    def __init__(self):
        
        self.data = MilkBasics().data
        self.WD  = WetDry()
        self.milking = self.create_weekly()
        self.create_write_to_csv()
        

    def create_weekly(self):
        lact1 = self.WD.milking_liters3
        grouping_key = lact1.index //7
        self.milking = lact1.groupby(grouping_key).mean()
        
        return self.milking
    
      
    
    
    def create_write_to_csv(self):
        self.milking.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\milking_thisLact.csv')
        
if __name__ == "__main__":
    ThisLactation()  