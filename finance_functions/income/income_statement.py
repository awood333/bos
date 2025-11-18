'''finance_functions.income.IncomeStatement'''

import inspect
import pandas as pd
# from date_range import DateRange
from container import get_dependency


class IncomeStatement:
    def __init__(self):
        print(f"IncomeStatement instantiated by: {inspect.stack()[1].filename}")
        self.net_income = None
        self.dtoc = None
        self.Capex = None

    def load_and_process(self):

        NR = get_dependency('net_revenue')
        self.net_income = NR.net_revenue_monthly

        MI = get_dependency('milk_income')
        self.dtoc = MI.income_monthly['dtoc']

        self.Capex = get_dependency('capex_basics')

        self.net_income = self.createNetIncome()
        self.write_to_csv()

    def createNetIncome(self):
        start_date = (2025, 1)
        ni = self.net_income

        inc1 = pd.concat(
            [ni, self.Capex.non_capex_pivot, self.dtoc],
            axis=1
        )

        inc1['net_income'] = inc1['net revenue'] - inc1['sum'] - inc1['dtoc']

        inc2 = inc1.loc[start_date:, :].copy()
        inc3 = pd.DataFrame(inc2.T)
        inc3['sum'] = inc3.sum(axis=1)

        self.net_income = inc3
        return self.net_income

    def write_to_csv(self):
        self.net_income.to_csv('F:\\COWS\\data\\PL_data\\net_income.csv')
        
    def createNetIncome(self):
        
        start_date = (2025, 1)
   
        ni = self.net_income
        
        inc1 = pd.concat(
            [ni, self.Capex.non_capex_pivot, self.dtoc],
            axis=1
            )
        
        inc1['net_income'] = inc1['net revenue'] - inc1['sum'] - inc1['dtoc']
        
        inc2        = inc1.loc[ start_date : ,:].copy()
        inc3        = pd.DataFrame(inc2.T)
        inc3['sum'] = inc3.sum(axis=1)
        
        self.net_income = inc3
   
        return self.net_income


    def write_to_csv(self):
        
        self.net_income.to_csv('F:\\COWS\\data\\PL_data\\net_income.csv')
        
        
if __name__ == "__main__":
    obj=IncomeStatement()
    obj.load_and_process()     
        
        