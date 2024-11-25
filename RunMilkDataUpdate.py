'''RunMilkDailyUpdate.py'''

from milk_functions.milkaggregates import MilkAggregates
# from insem_functions.tenday_age import TendayMilkingDays

class Main():
    def __init__(self):
            
        MilkAggregates()
        # TendayMilkingDays()

if __name__ == "__main__":
    Main()
    