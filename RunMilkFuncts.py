'''run_milk_functs.py'''

from milk_functions.lactations.lactation_this_lact  import ThisLactation
from milk_functions.lactations.lactations_weekly import WeeklyLactations
from milk_functions.lactations.lactation_basics import Lactation_basics

from milk_functions.milkaggregates import MilkAggregates
from status_functions.WetDry      import WetDry


def Main():
    
    ThisLactation()
    WeeklyLactations()
    Lactation_basics()
    
    MilkAggregates()  
    WetDry()


if __name__ == "__main__":
    Main()
    