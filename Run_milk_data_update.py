'''Run_milk_data_update.py'''

from milk_functions.milk_aggregates             import MilkAggregates
from milk_functions.lactation.lactation_basics  import LactationBasics
from milk_functions.lactation.this_lactation    import ThisLactation
from milk_functions.report_milk.report_milk_xlsx import ReportMilkXlsx


class RunMilkDataUpdate:
    def __init__(self):


        self.milk_aggregates    = MilkAggregates()
        self.lactation_basics   = LactationBasics()
        self.this_lactation     = ThisLactation()
        self.report_milk_xlsx   = ReportMilkXlsx()

        self.milk_aggregates.load_and_process()
        self.lactation_basics.load_and_process()
        self.this_lactation.load_and_process()
        self.report_milk_xlsx.load_and_process()


if __name__ == "__main__":
    RunMilkDataUpdate()
