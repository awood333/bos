'''RunMilkDailyUpdate.py'''

from milk_functions.milkaggregates import MilkAggregates
from milk_functions.lactations.lactation_this_lact import ThisLactation
from milk_functions.milking_groups import MilkingGroups

class Main():
    def __init__(self):
            
        MilkAggregates()
        ThisLactation()
        MilkingGroups()

if __name__ == "__main__":
    Main()
    