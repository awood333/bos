'''finance\\milk_net_revenue.py'''

import pandas as pd
from datetime import datetime
from feed_related.CreateStartDate import DateRange
from feed_related.feed_cost_basics import FeedCostBasics
from milk_functions.status_ids import StatusData

DR = DateRange()


class MilkNetRevenue:
    def __init__(self):
        
        self.FCB = FeedCostBasics()
        self.SD =  StatusData()
        self.DateRange = DateRange()

        self.income2, self.gtc, self.herd, self.feed_cost_by_group = self.DataLoader()
        self.income = self.calcMilkIncome()
        self.income_monthly = self.convert_daily_milk_income_to_monthly()
        self.net_revenue = self.create_net_revenue()
        self.milk_income_dash_vars  = self.convert_daily_milk_income_to_monthly()
        self.write_to_csv()
        
    def DataLoader(self):  

        income1 = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\base\\milk_income_base.csv')
        income1['datex'] = pd.to_datetime(income1['datex'])
        numcols = ['liters','gross_baht','tax','other']
        
        for col in numcols:
            income1[col] = income1[col].astype(float)
            
        income1.set_index('datex', inplace=True, drop=True)

        self.income2 =  income1
        
        self.gtc = pd.DataFrame(self.FCB.gtc)
        self.feed_cost_by_group = self.FCB.feedcostByGroup
        
        self.herd = self.SD.herd_monthly[['milkers','dry']]
        self.herd.loc[:,'milkers']  = self.herd['milkers']  .astype(int)
        self.herd.loc[:,'dry']      = self.herd['dry']      .astype(int)
        
        
        return self.income2, self.gtc, self.herd, self.feed_cost_by_group
    
    
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
        
        self.income = income3

        return self.income
    
    def convert_daily_milk_income_to_monthly(self):
        
        start = self.income.index[0]
        end     = DR.enddate_monthly
        rng     = pd.date_range(start, end, freq='D')
        
        income_daily1 = self.income.reindex(rng, method='bfill').ffill()
        income_daily  = income_daily1[['year','month','net']].copy()
        income_daily.loc[:,'days_in_month'] = income_daily.index.to_period('M').days_in_month
        
        self.income_monthly = income_daily.groupby(['year','month']).mean()
        
   # Convert the multi-index to integer
        self.income_monthly.index = self.income_monthly.index.set_levels(
            [self.income_monthly.index.levels[0].astype(int),
            self.income_monthly.index.levels[1].astype(int)]
            )
        
        return self.income_monthly
    
    def create_net_revenue(self):
        
        self.income_monthly.index.names     = ['year', 'month']
        self.feed_cost_by_group.index.names = ['year', 'month']
        self.herd.index.names               = ['year', 'month']
            
        
        nr1 = self.income_monthly.merge(self.feed_cost_by_group,
                                       how = 'left',
                                        left_index  = True,
                                       right_index = True
                                       )
        
        nr2 = nr1.loc[(2024,1): ].copy()
        nr3 = nr2.rename(columns={'net': 'income'})
         # Flatten the MultiIndex temporarily
        nr3 = nr3.reset_index()

        nr3['milkers agg cost'] = nr3['milkers'] * nr3['milkers cost']
        nr3['dry15 agg cost'] = nr3['dry 15pct'] * nr3['dry cost']
        nr3['total15 cost'] = nr3['milkers agg cost'] +  nr3['dry 15pct agg cost']
        
        
     
        nr3['net revenue'] = nr3['income'] - nr3['total15 cost']
        
         # Restore the MultiIndex
        nr3 = nr3.set_index(['year', 'month'])

        nr4 = nr3.merge(self.herd,
                        how = 'left',
                        left_index=True,
                        right_index=True
                        )
        
        self.net_revenue = nr4
        
        return self.net_revenue
        
            
    def get_dash_vars(self):
        self.milk_income_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.milk_income_dash_vars  
        
    
    def write_to_csv(self):
        self.income.to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income.csv')
        self.net_revenue.to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\net_revenue.csv')
        

if __name__ == '__main__':
    MilkNetRevenue()
