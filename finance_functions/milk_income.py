'''finance\\milk_income.py'''

import pandas as pd
from datetime import datetime
from CreateStartDate import DateRange
from feed_functions.feed_cost_basics import FeedCostBasics
from milk_functions.status_data import StatusData

DR = DateRange()


class MilkIncome:
    def __init__(self):
        
        self.FCB = FeedCostBasics()
        self.SD =  StatusData()
        self.DR = DateRange()

        self.income2         = self.DataLoader()
        self.income         = self.calcMilkIncome()
        self.income_monthly   =  self.convert_daily_milk_income_to_monthly()

        self.write_to_csv()
        
    def DataLoader(self):  

        income1 = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\data\\milk_income_data.csv')
        self.income2 = income1

        return self.income2
    
    
    def calcMilkIncome(self):

        income3 = self.income2

        income3['net']          = income3['gross_baht'] - income3['tax'] - income3['other']
        income3['gross/liter']  = income3['gross_baht'] / income3['liters']
        income3['net/liter']    = income3['net'] / income3['liters']

        income3['bonus']        = income3['gross/liter']  - income3['base']
        income3['bonus value']  = income3['bonus'] * income3['liters']
        
        cols = list(income3.columns)
        net_index = cols.index('net')
        cols.insert(net_index - 1, cols.pop(net_index))
        income3 = income3[cols]
        income3 = income3.set_index(['datex'])
        self.income = income3

        return self.income
    
    def convert_daily_milk_income_to_monthly(self):
        
        start = self.income.index[0]
        end     = DR.enddate_monthly
        rng     = pd.date_range(start, end, freq='D')
        
        # the last entry in income['datex'] might not be in the 'right' format
        self.income.index = pd.to_datetime(self.income.index, errors='coerce')
        
        income_daily = self.income.reindex(rng, method='bfill').ffill()
        
        income_daily.loc[:,'days_in_month'] = income_daily.index.to_period('M').days_in_month
        
        income_monthly1 = income_daily.groupby(['year','month']).mean()
        
        self.income_monthly = income_monthly1
        return self.income_monthly
    
    def write_to_csv(self):
        self.income     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_output.csv')
        

if __name__ == '__main__':
    MilkIncome()
