'''lactations_weekly.py'''

import pandas as pd
from MilkBasics import MilkBasics
from milk_functions.lactations import Lactations
from milk_functions.statusData import StatusData




class WeeklyLactations():
    def __init__(self):
        
        self.data   = MilkBasics().data
        self.L  = Lactations()
        self.SD = StatusData()
        self.alive_ids = self.SD.alive_ids.astype(int).to_list()

        (self.lact1, 
         self.lact2, 
         self.lact3, 
         self.lact4, 
         self.lact5    )         = self.create_308day()
        
        self.wk_lacts            = self.create_weekly()

        (self.lactation_wk_1,
         self.lactation_wk_2,
         self.lactation_wk_3,
         self.lactation_wk_4,
         self.lactation_wk_5 )      = self.create_separate_lactations()
        
        self.max_liters_list2   = self.create_max_liters()
        self.max_df            = self.unpack_max_liters()
        self.create_live_lactations()

        self.write_to_csv()


    def create_308day(self):

        self.lact1 = self.L.L1.iloc[:308,:].copy()
        self.lact2 = self.L.L2.iloc[:308,:].copy()
        self.lact3 = self.L.L3.iloc[:308,:].copy()
        self.lact4 = self.L.L4.iloc[:308,:].copy()
        self.lact5 = self.L.L5.iloc[:308,:].copy()

        return (self.lact1, self.lact2, self.lact3,
                self.lact4, self.lact5    )
        


    def create_weekly(self):
        var = (self.lact1, self.lact2, self.lact3, self.lact4, self.lact5    )
        self.wk_lacts = []
        j = 0
        for i in var:   # i is the entire lact df

            grouping_key    = i.index // 7 # the //7 creates the 7 row grouping
            var2       = i.groupby(grouping_key).mean()
            var2.index = var2.index.astype(int)
            self.wk_lacts.append(var2)
            j +=1

        return self.wk_lacts # nested list

    def create_separate_lactations(self):   # CONTAINS ALL COWS LACTATING
        wl = self.wk_lacts

        self.lactation_wk_1 = wl[0]
        self.lactation_wk_2 = wl[1]
        self.lactation_wk_3 = wl[2]
        self.lactation_wk_4 = wl[3]
        self.lactation_wk_5 = wl[4]

        return     (self.lactation_wk_1,
                    self.lactation_wk_2,
                    self.lactation_wk_3,
                    self.lactation_wk_4,
                    self.lactation_wk_5
                    )
        
    def create_max_liters(self):
        lacts = (
                    self.lactation_wk_2,
                    self.lactation_wk_3
                    )
        
        max_liters = {} 
        self.max_liters_list2 = max_liters_list1=[]
        
        max_liters = {} 
        for lact in lacts:
            for col in lact.columns:
                max_liters[col] = lact[col].max()
            max_liters_list1 = [max_liters]
        self.max_liters_list2.append(max_liters_list1)
        max_liters_list1=[]
        
        return self.max_liters_list2   
    
    def unpack_max_liters(self):
        max_list = self.max_liters_list2
        max_dfs = []
        for i in range(len(max_list)):
            max_dfs.append(pd.DataFrame(max_list[i], index=[0]))
    
        self.max_df = pd.concat(max_dfs, ignore_index=True)
    
        return self.max_df
    
    def create_live_lactations(self):
        
        self.live_lact_wk_1 = self.lactation_wk_1.loc[:,self.alive_ids]
        self.live_lact_wk_2 = self.lactation_wk_2.loc[:,self.alive_ids]
        self.live_lact_wk_3 = self.lactation_wk_3.loc[:,self.alive_ids]
        self.live_lact_wk_4 = self.lactation_wk_4.loc[:,self.alive_ids]
        self.live_lact_wk_5 = self.lactation_wk_5.loc[:,self.alive_ids]
        
        return [self.live_lact_wk_1, self.live_lact_wk_2, self.live_lact_wk_3,
                self.live_lact_wk_4, self.live_lact_wk_5]
              

    def write_to_csv(self):
        
        self.live_lact_wk_1.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\live_lact_wk_1.csv')
        
        self.lactation_wk_1.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\lactation_wk_1.csv')
        self.lactation_wk_2.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\lactation_wk_2.csv')
        self.lactation_wk_3.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\lactation_wk_3.csv')
        self.lactation_wk_4.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\lactation_wk_4.csv')
        self.lactation_wk_5.to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\lactation_wk_5.csv')
        
        self.max_df         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\weekly\\max.csv')
        

if __name__ == "__main__":
    WeeklyLactations()
