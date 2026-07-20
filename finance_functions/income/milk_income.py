'''finance\\milk_income.py'''
import inspect
import pandas as pd

from container import get_dependency

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class MilkIncome:
    def __init__(self):
        print(f"MilkIncome instantiated by: {inspect.stack()[1].filename}")
        self.FCBD = None
        self.DR = None
        self.startdate = None
        self.income = None
        self.income_daily = None
        self.income_monthly = None
        self.income_daily_last = None


    def load(self):
        self.FCBD = get_dependency('feedcost_by_group_by_day')
        self.DR  = get_dependency('date_range')
        self.MA = get_dependency('milk_aggregates')
        
        self.process()
        
    def process(self):
        
        self.feedcost   = self.FCBD.cost_by_group_by_day_df
        self.startdate  = self.DR.startdate
        self.weekly_total  = self.MA.weekly_total
        self.monthly_total = self.MA.monthly_total
 
        self.income_weekly  = self.create_income_weekly()
        self.income_monthly = self.create_income_monthly()
        
    
    def create_income_weekly(self):
        income_1 = self.weekly_total.copy()
        income_2 = income_1.drop(columns=['count'])
        income_3 = income_2.loc[self.startdate:, [income_2.columns[-1]]]
        income_4 = income_3['sum'] * 22
        self.income_weekly = pd.DataFrame(income_4, columns=['sum'])
        return self.income_weekly
    
    def create_income_monthly(self):
        income_1 = self.monthly_total.copy()
        income_2 = income_1.drop(columns=['count', 'week'])
        income_3 = income_2.loc[self.startdate:, [income_2.columns[-1]]]
        income_4 = income_3['sum'] * 22
        self.income_monthly = pd.DataFrame(income_4, columns=['sum'])
        return self.income_monthly

if __name__ == '__main__':
    obj=MilkIncome()
    obj.load()
    