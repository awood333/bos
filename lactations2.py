'''lactations2.py'''
import pandas as pd
import numpy as np

from lactations import Lactations
from insem_ultra import InsemUltraData

iud = InsemUltraData()
lac = Lactations()


all     = iud.all1
max_bd_cownum = lac.max_bd_cownum

class WeeklyLactations():
    def __init__(self):



        (self.lact1, 
         self.lact2, 
         self.lact3, 
         self.lact4, 
         self.lact5    )         = self.create_308day()
        
        self.wk_lacts            = self.create_weekly()

        (self.lactation_1,
         self.lactation_2,
         self.lactation_3,
         self.lactation_4,
         self.lactation_5 )      = self.create_separate_lactations()
        
        self.individual_lactations  = self.create_individual_lactations()
        self.write_to_csv()


    def create_308day(self):
                
        lact1a = lac.lact_1.iloc[:,:308].copy()
        lact1 = lact1a.T

        lact2a = lac.lact_2.iloc[:,:308].copy()
        lact2 = lact2a.T

        lact3a = lac.lact_3.iloc[:,:308].copy()
        lact3 = lact3a.T

        lact4a = lac.lact_4.iloc[:,:308].copy()
        lact4 = lact4a.T

        lact5a = lac.lact_5.iloc[:,:308].copy()
        lact5 = lact5a.T
        
        return (lact1, lact2, lact3, lact4, lact5    )


    def create_weekly(self):
        var = (self.lact1, self.lact2, self.lact3, self.lact4, self.lact5    )
        wk_lacts = []
        j = 0
        for i in var:

            grouping_key    = (i.index // 7) # the //7 creates the 7 row grouping
            var2       = i.groupby(grouping_key).mean()
            var2.index = var2.index.astype(int)
            wk_lacts.append(var[j])
            j +=1

        return wk_lacts

    def create_separate_lactations(self):   # CONTAINS ALL COWS LACTATING
        wl = self.wk_lacts

        lactation_1 = wl[0]
        lactation_2 = wl[1]
        lactation_3 = wl[2]
        lactation_4 = wl[3]
        lactation_5 = wl[4]

        return     (lactation_1,
                    lactation_2,
                    lactation_3,
                    lactation_4,
                    lactation_5
                    )
    



    def create_individual_lactations(self):
        cols = self.lactation_1.columns
        starts = range(1, 6)
        individual_lactations = {}

        for j in starts:
            cow_dict = {}
            for i in cols:
                lactation_col_name = 'lactation_' + str(j)
                lactation_data = getattr(self, lactation_col_name)[i].copy()
                cow_dict['cow_' + str(i + 1)] = lactation_data

            individual_lactations[j] = cow_dict

        wy15_lact1 = individual_lactations[1]['cow_15']
        wy15_lact2 = individual_lactations[2]['cow_15']
        wy15df = pd.DataFrame(wy15_lact1)
        wy15df['lact2'] = wy15_lact2

        return individual_lactations
        
    def write_to_csv(self):
            return None


