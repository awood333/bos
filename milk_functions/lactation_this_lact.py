'''milkingation1.py'''

import pandas as pd
from MilkBasics import MilkBasics
from milk_functions.WetDry import WetDry



class ThisLactation():
    def __init__(self):
        
        self.data = MilkBasics().data
        self.WD  = WetDry()
        
        (self.milking1, 
         self.milking2, 
         self.milking3, 
         self.milking4, 
         self.milking5    )         = self.create_308day()
        
        self.wk_milkings            = self.create_weekly()
        self.milking                 = self.concat_milking()
        self.create_write_to_csv()
        

    def create_308day(self):

        milking1 = self.WD.milking_1.iloc[:308,:].copy()
        milking2 = self.WD.milking_2.iloc[:308,:].copy()
        milking3 = self.WD.milking_3.iloc[:308,:].copy()
        milking4 = self.WD.milking_4.iloc[:308,:].copy()
        milking5 = self.WD.milking_5.iloc[:308,:].copy()

        return (milking1, milking2, milking3, milking4, milking5    )
    
    
    def create_weekly(self):
        var = (self.milking1, self.milking2, self.milking3, self.milking4, self.milking5    )
        self.wk_milkings = []
        j = 0
        for i in var:   # i is the entire milking df

            grouping_key    = i.index // 7 # the //7 creates the 7 row grouping
            var2       = i.groupby(grouping_key).mean()
            var2.index = var2.index.astype(int)
            self.wk_milkings.append(var2)
            j +=1

        return self.wk_milkings # nested list
    
    
    def concat_milking(self):
        
        self.milking = pd.concat([self.wk_milkings[0], self.wk_milkings[1], self.wk_milkings[2], self.wk_milkings[3], self.wk_milkings[4]], axis=1)
        
        return self.milking
    
    
    def create_write_to_csv(self):
        self.milking.to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking.csv')  