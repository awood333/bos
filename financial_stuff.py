'''
financial_stuff.py
'''
import pandas as pd
from IPython.display import display
from feed_cost import FeedCost 
from startdate_funct import CreateStartdate



class FinancialStuff:
    def __init__(self):
        
        self.sdf = CreateStartdate()
        self.fc  = FeedCost()
        
        
        self.f1      = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',   index_col='datex',header=0,  parse_dates=['datex'])
        self.income1  = pd.read_csv('F:\\COWS\\data\\csv_files\\milk_income.csv',       index_col='datex',header=0,  parse_dates=['datex'])
        self.bkk1        = pd.read_excel('F:\\COWS\\data\\BKKBankFarmAccount.xlsm',      index_col='datex',header=0,
                                        parse_dates=['datex'], sheet_name='consol statement')
        self.bkk1.index = pd.to_datetime(self.bkk1.index, format=self.sdf.date_format)
        
        self.maxdate     = self.f1.index.max()  
        # stopdate    = '2023-10-20'
        self.stopdate    = self.maxdate
        
  

        # functions
        
        # self.monthly_income         = self.create_monthly_income() 
        # self.gross_income           = self.create_gross_income()
        self.bkk_feed, self.bkk_all    = self.create_monthly_cost()
        
        # self.create_write_to_csv() 
        
        
    def create_monthly_cost(self):
                
        bkk2023 = self.bkk1[   ( 
              (self.bkk1['year'] == 2023 )
            & (self.bkk1['month'] >= 6)
            )  ]
        
        bkk2 = bkk2023.reset_index(drop=True)
        
        
        bkk3 = bkk2.drop(columns=['day','transaction',
            'credit','descr 3','capex','brahman'])
        
        bkk_feed = bkk3[bkk3['descr 1'] == 'feed']
        
        bkk_feed = pd.pivot_table(
            bkk_feed, 
            index = ['year','month'],
            values = 'debit',
            columns= 'descr 2',
            aggfunc= 'sum')

        
        bkk_all = pd.pivot_table(
            bkk3, 
            index = ['year','month'],
            values = 'debit',
            columns= 'descr 1',
            aggfunc= 'sum'
            )
        
        bkk_all1 = bkk_all.drop(columns=['sale', 'nonfarm']) 

        bkk_all1.loc[:,'total cost']    = bkk_all1.sum(axis = 1)    # last col
        bkk_all1.loc['sum']             = bkk_all1.sum(axis = 0)    # last row

        bkk_all = bkk_all1
        
        return bkk_feed, bkk_all
        

        
        


    # def create_monthly_income(self):
    #     new_data = {
    #         'datex': ['2023-7-15','2023-9-30'],
    #         'liters': [4250.4, 7833],
    #         'gross_baht': [85008, 156660],
    #         'net_baht': [84000, 155000] ,
    #         'per_liter': [20, 20]      
    #         }
        
    #     new_rows = pd.DataFrame(new_data)
    #     new_rows['datex'] = pd.to_datetime(new_rows['datex'], format=self.sdf.date_format)
    #     new_rows.set_index('datex', inplace=True)

    #     adj_income = pd.concat([self.income1, new_rows])
    #     adj_income.sort_index(inplace=True)
    #     adj_income['year']  = adj_income.index.year
    #     adj_income['month'] = adj_income.index.month
    #     adj_income.drop('per_liter', axis=1, inplace=True)

    #     monthly_milk_income1     = adj_income.groupby(['year','month']).sum()
    #     monthly_milk_income1.reset_index( inplace=True)
    #     monthly_milk_income = pd.DataFrame(monthly_milk_income1)
    #     return monthly_milk_income





    # def create_gross_income(self):
        
    #     feed1 = self.fc.monthly_feedcost.reset_index()
    #     feed = feed1[['month','year','total feed cost']]
    #     income = self.monthly_income[['month','year','net_baht']]
    #     gross = income.merge(feed, on=['month', 'year'], how='outer')
        
    #     gross['gross income']  = (gross['net_baht'] -  gross['total feed cost'])
    #     gross_income = pd.DataFrame(gross)
    #     gross_income = gross_income.sort_values(by=['year', 'month'])
    #     gross_income.rename(columns={'net_baht': 'income', 'total feed cost': 'feed cost'}, inplace=True)
        
    #     return gross_income





    # def create_write_to_csv(self):
        
        # self.gross_income.to_csv('F:\\COWS\\data\\PL_data\\gross_income.csv')

