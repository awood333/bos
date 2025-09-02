'''RunMilkDailyUpdate.py'''

from milk_functions.milk_aggregates  import MilkAggregates
from milk_functions.lactation.this_lactation import ThisLactation
from milk_functions.milking_groups  import MilkingGroups
from milk_functions.report_milk.milk_dash_app    import run_dash_app
from milk_functions.report_milk.report_milk_xlsx_version import ReportMilkXlsx

class Main():
    def __init__(self):
            
        MA = MilkAggregates()
        ThisLactation()
        MG = MilkingGroups(milk_aggregates=MA)
        ReportMilkXlsx()
        run_dash_app(milk_aggregates=MA, milking_groups=MG )


if __name__ == "__main__":
    Main()
    