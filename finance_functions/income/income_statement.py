'''finance_functions.income.IncomeStatement'''

import inspect
import pandas as pd
# from date_range import DateRange
from container import get_dependency


class IncomeStatement:
    def __init__(self):
        print(f"IncomeStatement instantiated by: {inspect.stack()[1].filename}")
        self.net_revenue=None
        self.net_income = None
        self.net_income_details = None
        self.Capex = None

    def load_and_process(self):

        NR = get_dependency('net_revenue')
        self.net_revenue = NR.net_revenue_monthly[["net revenue", "daily liters"]]

        # MI = get_dependency('milk_income')
        # self.dtoc = MI.income_monthly['dtoc']

        self.Capex = get_dependency('capex_basics')

        self.net_income, self.net_income_details = self.createNetIncome()
        self.write_to_csv()

    def createNetIncome(self):

        nr = self.net_revenue
        cost_details = self.Capex.non_capex_pivot
        cost = self.Capex.non_capex_pivot['cost sum']
        # Include 'daily liters' in the result
        ni = pd.concat([
            nr['net revenue'].to_frame('net revenue'),
            nr['daily liters'].to_frame('daily liters'),
            cost.rename('cost').to_frame('cost')
        ], axis=1)
        ni['net_income'] = ni['net revenue'] - ni['cost']

        ni_details = pd.concat([
            nr['net revenue'].to_frame('net revenue'),
            nr['daily liters'].to_frame('daily liters'),
            cost_details
        ], axis=1)
        ni_details['net income'] = ni_details['net revenue'] - ni_details['cost sum']

        start_date = (2025, 1)
        ni = ni.loc[start_date:]
        ni_details = ni_details.loc[start_date:]
        # Convert 'net revenue' and 'net_income' columns to numeric before rounding
        for col in ['net revenue', 'net_income']:
            if col in ni.columns:
                ni[col] = pd.to_numeric(ni[col], errors='coerce')
            if col in ni_details.columns:
                ni_details[col] = pd.to_numeric(ni_details[col], errors='coerce')
        # Also ensure 'net income' in ni_details is numeric and rounded
        if 'net income' in ni_details.columns:
            ni_details['net income'] = pd.to_numeric(ni_details['net income'], errors='coerce').round(0)
        ni = ni.round(0)
        ni_details = ni_details.round(0)
        self.net_income = ni
        self.net_income_details = ni_details
        return self.net_income, self.net_income_details

    def write_to_csv(self):
        self.net_income.to_csv('F:\\COWS\\data\\PL_data\\net_income.csv')
        self.net_income_details.to_csv('F:\\COWS\\data\\PL_data\\net_income_details.csv')                
        
if __name__ == "__main__":
    obj=IncomeStatement()
    obj.load_and_process()     
        
        