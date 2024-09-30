'''RunMilkDailyUpdate.py'''

from milk_functions.milkaggregates import MilkAggregates
from milk_functions.WetDryBasics import WetDryBasics
from milk_functions.WetDry import WetDry

class Main():
    def __init__(self):
            
        MilkAggregates()
        WetDry()

if __name__ == "__main__":
    Main()
    