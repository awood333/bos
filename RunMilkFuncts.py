'''run_milk_functs.py'''

from milk_functions.lactation.this_lactation  import ThisLactation
from milk_functions.lactation.weekly_lactations import WeeklyLactations
from milk_functions.lactation.lactation_basics import LactationBasics

from milk_functions.milk_aggregates import MilkAggregates
from status_functions.wet_dry      import WetDry


def Main():
    
    ThisLactation()
    WeeklyLactations()
    LactationBasics()
    MilkAggregates()  
    WetDry()


if __name__ == "__main__":
    Main()
    