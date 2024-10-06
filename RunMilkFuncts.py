'''run_milk_functs.py'''

from milk_functions.lactation_this_lact  import ThisLactation
from milk_functions.lactations_weekly import WeeklyLactations
from milk_functions.lactation_basics import Lactation_basics

from milk_functions.WetDry      import WetDry
from milk_functions.check_laststop import CheckLastStop
from milk_functions.milkaggregates import MilkAggregates
from milk_functions.days_of_milking import DaysOfMilking

def Main():
    
    ThisLactation()
    WeeklyLactations()
    Lactation_basics()
    
    WetDry()
    CheckLastStop()
    MilkAggregates()
    DaysOfMilking()


if __name__ == "__main__":
    Main()
    