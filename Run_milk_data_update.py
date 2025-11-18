'''Run_milk_data_update.py'''
from container import get_dependency

class RunMilkDataUpdate:
    def __init__(self):

        self.milk_basics = get_dependency('milk_basics')
        self.wet_dry = get_dependency('wet_dry')
        self.insem_ultra_basics = get_dependency('insem_ultra_basics')
        self.insem_ultra_data = get_dependency('insem_ultra_data')
        self.milk_aggregates = get_dependency('milk_aggregates')
        self.this_lactation = get_dependency('this_lactation')
        self.report_milk_xlsx = get_dependency('report_milk_xlsx')
        # self.run_milk_dash_app = get_dependency('run_milk_dash_app')

if __name__ == "__main__":
    RunMilkDataUpdate()
