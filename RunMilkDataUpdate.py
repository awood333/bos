'''RunMilkDailyUpdate.py'''

from milk_functions.milkaggregates import MilkAggregates
from milk_functions.lactations.lactation_this_lact import ThisLactation

class Main():
    def __init__(self):
            
        MilkAggregates()
        ThisLactation()

if __name__ == "__main__":
    Main()
    