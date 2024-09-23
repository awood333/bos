'''create_milk_revenue.py'''

import pandas as pd
from datetime import datetime
from milk_functions.milkaggregates import MilkAggregates 

class Revenue:
    def __init__(self):
        
        MA = MilkAggregates()
        self.cow_count = MA.monthly
        self.cow_count.set_index (['year','month'], inplace=True)
        
        
        self.start = MA.start
        self.stop  = MA.stop
        
        income1 = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\milk_income.csv', header=0)
        income1['datex'] = pd.to_datetime(income1['datex'])
        income1.index = income1['datex']

        self.income2 = income1

        self.income = self.reindex_income()
        self.create_merge_income_cowcount()
    
    def reindex_income(self):
        
        rng = pd.date_range(self.start, self.stop, freq='D' )
        income3 = self.income2.reindex(rng, method='ffill').bfill()

        income4 = income3[['net_baht_TGS']].copy()
        income4['year'] = income4.index.year
        income4['month'] = income4.index.month

        self.income =  income4.groupby(['year','month'],as_index=False).agg({'net_baht_TGS':'mean'})
        self.income.set_index (['year','month'], inplace=True)
        return self.income
    
    def create_merge_income_cowcount(self):
        x = self.cow_count.merge(right = self.income, how='outer', left_index=True, right_index=True)
        print(x)
        
if __name__ == "__main__":
    revenue = Revenue()
    revenue.create_merge_income_cowcount()
         