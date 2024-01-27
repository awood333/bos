'''lactations2.py'''
import pandas as pd
import numpy as np

from lactations import Lactations
from insem_ultra import InsemUltraData

iud = InsemUltraData()
lac = Lactations()

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


all     = iud.all1

class WeeklyLactations():
    def __init__(self):
        self.wk_lacts            = self.create_weekly()

        (self.lactation_1,
         self.lactation_2,
         self.lactation_3,
         self.lactation_4,
         self.lactation_5)      = self.create_separate_lactations()
        
        self.individual             = self.create_individual_lactations()


    def create_weekly(self):
        var = (lact1, lact2, lact3, lact4, lact5    )
        wk_lacts = []
        j = 0
        for i in var:

            grouping_key    = (i.index // 7) # the //7 creates the 7 row grouping
            var2       = i.groupby(grouping_key).mean()
            var2.index = var2.index.astype(int)
            wk_lacts.append(var[j] + j)
            j +=1

        return wk_lacts

    def create_separate_lactations(self):
        wl = self.wk_lacts
        
        lactation_1 = wl[0]
        lactation_2 = wl[1]
        lactation_3 = wl[2]
        lactation_4 = wl[3]
        lactation_5 = wl[4]
        # lactation_6 = wl[5]

        return     (lactation_1,
                    lactation_2,
                    lactation_3,
                    lactation_4,
                    lactation_5
                    # lactation_6
                    )
    

    def create_individual_lactations(self):

        i=0
        cols = self.lactation_1.columns
        individual_lactations = {}
        for i in cols:

             individual_lactations.update({'cow_' + str(i + 1): 
                                           self.lactation_1[i].copy()})
        return individual_lactations


