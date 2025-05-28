import pandas as pd
import numpy as np
from MilkBasics import MilkBasics
from milk_functions.lactations import Lactation_basics
from milk_functions.lactations import Lactations
from milk_functions.lactations import ThisLactation
from milk_functions.lactations import WeeklyLactations


class LactationsLogStandard:
    def __init__(self):
        self.MB = MilkBasics()
        self.LB = Lactation_basics() 
        self.L = Lactations() 
        self.LTL = ThisLactation()  
        self.LW = WeeklyLactations()  # Ensure proper instantiation

    def create_log_standard(self):
        m1 = pd.read_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\milking.csv", index_col=0)
        m1_log = np.log(m1)
        return m1_log

if __name__ == "__main__":
    LLS = LactationsLogStandard()
    log_standard = LLS.create_log_standard()
