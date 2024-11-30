'''
financial_stuff.py
'''
import pandas as pd
# import numpy as np
# from IPython.display import display
from feed_cost import FeedCost 
from startdate_funct import CreateStartdate



class FinancialStuff:
    def __init__(self):
        
        self.sdf = CreateStartdate()
        self.fc  = FeedCost()
        
        
        self.f1      = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',   index_col='datex',header=0,  parse_dates=['datex'])
        self.monthly_income  = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\milk_income.csv',       index_col='datex',header=0,  parse_dates=['datex'])
        self.bkk1        = pd.read_excel('F:\\COWS\\data\\BKKBankFarmAccount.xlsm',      index_col='datex',header=0,
                                        parse_dates=['datex'], sheet_name='consol statement')
        self.bkk1.index = pd.to_datetime(self.bkk1.index, format=self.sdf.date_format)
        
        self.maxdate     = self.f1.index.max()  
        self.startdate        = self.sdf.startdate 
        self.stopdate    = self.maxdate
        
        self.idx        = pd.date_range(self.startdate, self.stopdate, freq='D')
      

  

        # functions

        self.bkk_feed, self.bkk_all, self.bkk_all_xFeed = self.create_monthly_feedcost_actual()
        
        self.monthly_income             = self.adjust_bimonthly_income()
        self.net_revenue                = self.create_net_revenue()
        self.create_write_to_csv() 
        
        
    def create_monthly_feedcost_actual(self):
        
        bkk2023 = self.bkk1[   ( 
              (self.bkk1['year'] == 2023 )
            & (self.bkk1['month'] >= 6)
            & (self.bkk1['capex'].isnull() )
            & (self.bkk1['brahman'].isnull())   
            )  ]
        
        bkk2023 = bkk2023.set_index(['year', 'month'])
        bkk2    = bkk2023.reset_index()
        
        
        bkk3 = bkk2.drop(columns=['day','transaction',
            'credit','descr 3','capex','brahman'])
        
        bkk_feed = bkk3[bkk3['descr 1'] == 'feed']
        
        bkk_feed = pd.pivot_table(
            bkk_feed, 
            index = ['year','month'],
            values = 'debit',
            columns= 'descr 2',
            aggfunc= 'sum')

        bkk_feed = bkk_feed.reset_index()
        
        bkk_all = pd.pivot_table(
            bkk3, 
            index = ['year','month'],
            values = 'debit',
            columns= 'descr 1',
            aggfunc= 'sum'
            )
        
        bkk_all = bkk_all.reset_index()
        
        bkk_all1 = bkk_all.drop(columns=['sale', 'nonfarm', '?']) 
        bkk_all_xFeed =  bkk_all.drop(columns=['sale', 'nonfarm', 'feed']) 

        bkk_all1.loc     [:,'total cost']    = bkk_all1.sum(axis = 1)    # last col
        bkk_all_xFeed.loc[:,'total xfeed cost']    = bkk_all_xFeed.sum(axis = 1)
        # bkk_all1.loc['sum']             = bkk_all1.sum(axis = 0)    # last row

        bkk_all = bkk_all1
        
        return bkk_feed, bkk_all, bkk_all_xFeed
    
   
    def adjust_bimonthly_income(self):
        minc1 = self.monthly_income.groupby(['year', 'month']).sum() 
        minc2 = minc1['net_baht']
        minc = pd.DataFrame(minc2)
        minc.reset_index()
        monthly_income = minc2
        return monthly_income
    
   

    def create_net_revenue(self):      # revenue after
        
        feed1 = self.fc.monthly_feedcost.reset_index()
        feed = feed1[['month','year','total feed cost']]
        # revenue = self.monthly_income[['month','year','net_baht']]  
        net = feed.merge(self.monthly_income, on=['month', 'year'], how='outer')
        
        net['net revenue']  = (net['net_baht'] -  net['total feed cost'])
        net_revenue = pd.DataFrame(net)
        net_revenue = net_revenue.sort_values(by=['year', 'month'])
        net_revenue.rename(columns={'net_baht': 'revenue', 'total feed cost': 'feed cost'}, inplace=True)
        
        return net_revenue


    # def create_gross_profit(self):
    #     other = self.bkk_all_xFeed['total xfeed cost']
    
    def create_write_to_csv(self):
        
        self.net_revenue.to_csv('F:\\COWS\\data\\PL_data\\net_revenue.csv')
        self.bkk_all_xFeed.to_csv('F:\\COWS\\data\\PL_data\\bkk_all_xFeed.csv')
        self.bkk_all.to_csv('F:\\COWS\\data\\PL_data\\bkk_all.csv')
        self.bkk_feed.to_csv('F:\\COWS\\data\\PL_data\\bkk_feed.csv')
        self.monthly_income.to_csv('F:\\COWS\\data\\PL_data\\adj_monthly_income.csv')
        

