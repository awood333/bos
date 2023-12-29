'''
cow_PL.py
'''
import pandas as pd
import numpy as np
from startdate_funct import CreateStartdate
# import status
# import milkaggregates
# import feed_cost
# import status_module_adjustment
# import financial_stuff

class Cow_PL:
    def __init__(self):
        self.csd = CreateStartdate()
        
        
        self.cp = pd.read_csv('F:\\COWS\\data\\status\\combined_status_col.csv', index_col='datex' , 
                            parse_dates=['datex'])
        self.a  = pd.read_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\group_a_daily_feed_costpercow.csv', index_col='datex' , 
                            parse_dates=['datex'])
        self.b  = pd.read_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\group_b_daily_feed_costpercow.csv', index_col='datex' , 
                            parse_dates=['datex'])
        self.d  = pd.read_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\dry_daily_feed_costpercow.csv', index_col='datex' , 
                            parse_dates=['datex'])
        
        
        self.f1     = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',index_col=0, header=0, 
                            parse_dates=['datex'])
        self.mms    = pd.read_csv('F:\\COWS\\data\\milk_data\\totals\\monthly_sum.csv',index_col=None, header=0)
        
        self.minc   = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\milk_income.csv',index_col=None, header=0, 
                            parse_dates=['datex'])
        self.bd     = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', header=0, index_col='WY_id',
                            parse_dates=['birth_date','death_date','arrived', 'adj_bdate'])
        
        
        self.maxdate     = self.f1.index.max()   
        self.stopdate    = self.maxdate
        self.startdate   = self.csd.startdate
        self.date_format = self.csd.date_format
        self.date_range  = pd.date_range(self.startdate, self.stopdate, freq='D')

        self.f = self.f1.loc[self.date_range].copy()    #partition the milk dbase
    
        
        # functions:

        self.livecowsmask                   = self.create_livecowsmask()
        self.cow_daily_cost, self.x4        = self.create_cow_daily_cost()
        self.daily_price                    = self.create_daily_price()
        self.daily_income                   = self.create_daily_income()
        
        self.monthly_liters, self.monthly_income   = self.create_monthly_income()
        
        self.monthly_cost                   = self.create_monthly_cost()
        self.net_revenue                    = self.create_net_revenue()
        self.create_write_to_csv()
        
    def create_livecowsmask(self):
        
        mask1 = self.bd.loc[(self.bd['death_date'] 
                             > self.startdate) 
                            | self.bd['death_date'].isnull() 
                            ]
        
        mask2 = mask1.index.tolist()
        mask2a = [str(i) for i in mask2]
        livecowsmask = mask2a.copy()
        return livecowsmask
        
        

    def create_cow_daily_cost(self):
        a2 = self.a.iloc[:,-1:]     # 'a' is df of all the feeds costs - thisis the total amt
        b2 = self.b.iloc[:,-1]
        d2 = self.d.iloc[:,-1]
        
        # create blank df from 'f'
        x4 = pd.DataFrame(index = self.f.index, columns=self.f.columns )
        
        for i in range(len(x4.index)):
            for j in range(len(x4.columns)):
                dl = x4.index[i]
                c  = x4.columns[j]
                x4.loc[dl,c] =  np.where( self.cp.loc[dl, c] == 'A', a2.loc[dl],
                                    np.where( self.cp.loc[dl, c]  == 'B', b2.loc[dl],
                                    np.where( self.cp.loc[dl, c] == 'D', d2.loc[dl], '')))
                
        x4.replace('', np.nan, inplace=True)
        x5= x4 = x4.astype(float)
        cow_daily_cost = x4
        return cow_daily_cost, x4
    
    
        
    def create_daily_price(self):
        
        dp = self.minc.iloc[:,:].copy()
        dp.set_index('datex', inplace=True)
        
        dp1 = dp.reindex(self.date_range, method='bfill')
        dp2 = dp1['net_price']
        daily_price = pd.DataFrame(dp2, columns=['net_price'])
        return daily_price
    
    
    
    def create_daily_income(self):
     
        if not self.f.index.equals(self.daily_price.index):
            raise ValueError('indices not same')
 
        di = pd.DataFrame(index=self.f.index)
        
        di = pd.concat([self.f[col] * self.daily_price['net_price'] for col in self.f.columns ], axis=1)
        di.columns = self.f.columns
        
        daily_income = di
        return daily_income


    def create_monthly_income(self):
        
        mask = self.livecowsmask
        
        f2 = self.f.loc[:, mask].copy()
        f2['year'] = f2.index.year
        f2['month'] = f2.index.month
        
        di2 = self.daily_income.loc[:, mask].copy()
        di2['year'] = di2.index.year
        di2['month'] = di2.index.month
        
        monthly_liters = f2.groupby(['year', 'month'], as_index=False).sum()
        monthly_income = di2.groupby(['year', 'month'], as_index=False).sum()
        return monthly_liters, monthly_income
            
    

    
    def create_monthly_cost(self):
        
        mask = self.livecowsmask        
        x5 = self.x4.loc[:,mask].copy()
        x5['year'] = x5.index.year
        x5['month'] = x5.index.month

        monthly_cost = x5.groupby(['year', 'month'], as_index=False).sum()
        # monthly_cost[['year', 'month']] = monthly_cost[['year', 'month']].astype(str)

        return monthly_cost
    
 
    
    def create_net_revenue(self):
        
        minc = self.monthly_income.drop(columns=['year', 'month'])
        mcost = self.monthly_cost.drop(columns=['year', 'month'])

        net1 = minc - mcost

        net1.loc['sum /cow'] = net1.sum(axis=0)
        net1['sum'] = net1.sum(axis=1)
    
        net1[['year', 'month']] = self.monthly_cost[['year', 'month']]

        net_revenue = net1   
        return net_revenue
        
        
     
    def create_write_to_csv(self):
        
        self.net_revenue        .to_csv('F:\\COWS\\data\\PL_data\\cow_net_revenue.csv')
        self.monthly_cost       .to_csv('F:\\COWS\\data\\PL_data\\monthly_cost.csv')
        self.monthly_liters     .to_csv('F:\\COWS\\data\\PL_data\\monthly_liters.csv')
        self.monthly_income     .to_csv('F:\\COWS\\data\\PL_data\\monthly_income.csv')



        return None
        
        
        
        
        
        
        
        
        
    
    
