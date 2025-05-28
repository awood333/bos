'''finance_functions.income.IncomeStatement'''

# import pandas as pd
from finance_functions.PL.NetRevenue import NetRevenue
from finance_functions.capex.CapexBasics import CapexBasics


class IncomeStatement:
    def __init__(self):
        
        self.NR = NetRevenue()
        self.Capex = CapexBasics()
        self.net_income = self.createNetIncome()
        self.write_to_csv()
        
    def createNetIncome(self):
        inc1 = self.NR.net_revenue_monthly
        inc1['cost']    = self.Capex.non_capex_pivot['sum']
        inc1['net_income'] = inc1['net revenue'] - inc1['cost']
        
        self.net_income = inc1
   
        return self.net_income


    def write_to_csv(self):
        
        self.net_income.to_csv('F:\\COWS\\data\\PL_data\\net_income.csv')
        
        
if __name__ == "__main__":
    IncomeStatement()
        
        