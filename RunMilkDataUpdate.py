'''RunMilkDailyUpdate.py'''

from milk_functions.milkaggregates  import MilkAggregates
from milk_functions.lactations.lactation_this_lact import ThisLactation
from milk_functions.milking_groups  import MilkingGroups
from milk_functions.report_milk.milk_dash_app    import run_dash_app

class Main():
    def __init__(self):
            
        MA = MilkAggregates()
        ThisLactation()
        MG = MilkingGroups(milk_aggregates=MA)
        run_dash_app(milk_aggregates=MA, milking_groups=MG )


if __name__ == "__main__":
    Main()
    