'''finance_functions.income.IncomeStatement'''

import inspect
import pandas as pd
from date_range import DateRange
from finance_functions.PL.NetRevenue import NetRevenue
from finance_functions.income.MilkIncome import MilkIncome
from finance_functions.capex.CapexBasics import CapexBasics


class IncomeStatement:
    def __init__(self, date_range=None, net_revenue=None, milk_income=None, capex_basics=None):
        
        print(f"IncomeStatement instantiated by: {inspect.stack()[1].filename}")
        self.DR = date_range or DateRange()
        self.NR = net_revenue or NetRevenue()
        self.net_income = self.NR.net_revenue_monthly
        
        MI = milk_income or MilkIncome()
        self.dtoc = MI.income_monthly['dtoc']
        
        self.Capex = capex_basics or CapexBasics()
        
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
        
        inc2        = inc1.loc[ start_date : ,:].copy()
        inc3        = pd.DataFrame(inc2.T)
        inc3['sum'] = inc3.sum(axis=1)
        
        self.net_income = inc3
   
        return self.net_income


    def write_to_csv(self):
        
        self.net_income.to_csv('F:\\COWS\\data\\PL_data\\net_income.csv')
        
        
if __name__ == "__main__":
    IncomeStatement()
        
        