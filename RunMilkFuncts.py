'''run_milk_functs.py'''

# from milk_functions.lactation_this_lact  import ThisLactation
# from milk_functions.lactations_weekly import WeeklyLactations
from milk_functions.lactation_basics import Lactation_basics
from milk_functions.milk_agg_allx_merge import MilkAggAllxMerge

from milk_functions.WetDry      import WetDry
from milk_functions.milkaggregates import MilkAggregates

def Main():
    
    # ThisLactation()
    # WeeklyLactations()
    Lactation_basics()
    MilkAggregates()  
    MilkAggAllxMerge()
    
    WetDry()


if __name__ == "__main__":
    Main()
    