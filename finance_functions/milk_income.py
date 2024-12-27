'''finance\\milk_income.py'''

import pandas as pd
# from datetime import datetime
from CreateStartDate import DateRange
from feed_functions.feed_cost_basics import FeedCostBasics
from milk_functions.statusData import StatusData

DR = DateRange()

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class MilkIncome:
    def __init__(self):
        
        self.FCB = FeedCostBasics()
        self.SD =  StatusData()
        self.DR = DateRange()

        self.income2         = self.DataLoader()
        self.income_net, self.income_full         = self.calcMilkIncome()
        self.income_daily, self.income_monthly   =  self.create_reindexed_daily_monthly()

        self.write_to_csv()
        
    def DataLoader(self):  

        income1 = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\data\\milk_income_data.csv')
        income1     .to_csv('E:\\COWS\\data_backup\\milk_income_backup\\milk_income_data'+tdy+'.csv')
        self.income2 = income1

        return self.income2
    
    
    def calcMilkIncome(self):

        income3 = self.income2

        income3['net']          = income3['gross_baht'] - income3['tax'] - income3['other']
        income3['gross/liter']  = income3['gross_baht'] / income3['liters']
        income3['net/liter']    = income3['net'] / income3['liters']

        income3['bonus']        = income3['gross/liter']  - income3['base']
        income3['bonus value']  = income3['bonus'] * income3['liters']
        
        # cols = list(income3.columns)
        # net_index = cols.index('net')   #returns col num (for position)
        # cols.insert(net_index - 1, cols.pop(net_index))
        # income3 = income3[cols]
        # income3 = income3.set_index(['datex'])
        
      
        income3['daily_avg_net'] = income3['net'] / income3['day']
        income4 = income3[['datex','daily_avg_net']]
        self.income_net = income4
        self.income_full = income3

        return self.income_net, self.income_full
    
    def create_reindexed_daily_monthly(self):
        
        start = self.income_net.index[0]
        end_daily     = DR.enddate_monthly
        end_monthly   = DR.enddate_daily
        rng_daily     = self.DR.date_range_daily
        rng_monthly   = self.DR.date_range_monthly
        
        self.income_net.loc[:,'datex'] = pd.to_datetime(self.income_net['datex'], errors='coerce')
        self.income2 = self.income_net.set_index('datex', drop=True)
        
        
        
        income_daily    = self.income2.reindex(rng_daily, method='bfill').ffill()
        income_daily2 = income_daily
        income_daily2['year'] = income_daily.index.year
        income_daily2['month'] = income_daily.index.month
        self.income_daily = income_daily2
        

        income_monthly1 = income_daily.groupby(['year','month']).mean()
        
        self.income_monthly = income_monthly1.loc[2024:,:]
        
        
        return self.income_daily, self.income_monthly
    
    def write_to_csv(self):
        self.income_net     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_output.csv')
        self.income_net     .to_csv('E:\\COWS\\data_backup\\milk_income_backup\\milk_income_'+tdy+'.csv')
        self.income_full    .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_full.csv')

if __name__ == '__main__':
    MilkIncome()
