'''lactations2.py'''

from MilkBasics import MilkBasics
from milk_functions.WetDry import WetDry



class WeeklyLactations():
    def __init__(self):
        
        self.data   = MilkBasics().data
        self.WD     = WetDry()

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

        # self.individual_lactations  = self.create_individual_lactations()
        self.write_to_csv()


    def create_308day(self):

        lact1 = WD.lact_1.iloc[:308,:].copy()
        lact2 = WD.lact_2.iloc[:308,:].copy()
        lact3 = WD.lact_3.iloc[:308,:].copy()
        lact4 = WD.lact_4.iloc[:308,:].copy()
        lact5 = WD.lact_5.iloc[:308,:].copy()

        return (lact1, lact2, lact3, lact4, lact5    )


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

        self.lactation_1 = wl[0]
        self.lactation_2 = wl[1]
        self.lactation_3 = wl[2]
        self.lactation_4 = wl[3]
        self.lactation_5 = wl[4]

        return     (self.lactation_1,
                    self.lactation_2,
                    self.lactation_3,
                    self.lactation_4,
                    self.lactation_5
                    )

    def write_to_csv(self):
        return None
