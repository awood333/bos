'''RunMilkDailyUpdate.py'''

from milk_functions.milkaggregates  import MilkAggregates
from milk_functions.lactations.lactation_this_lact import ThisLactation
from milk_functions.milking_groups  import MilkingGroups
# from milk_functions.report_milk    import ReportMilk

class Main():
    def __init__(self):
            
        MilkAggregates()
        ThisLactation()
        MilkingGroups()
        # ReportMilk()

if __name__ == "__main__":
    Main()
    