'''feed_related\\feed_cost_basics.py'''

import os       #don't erase
from datetime import datetime
import pandas as pd


from CreateStartDate import DateRange
from milk_functions.status_data import StatusData
from MilkBasics import MilkBasics 

from utilities.logging_setup import LoggingSetup

class DataLoader:
    def __init__(self,base_path):
        

        self.base_path = base_path

    def load_csv(self, filename):
        path = os.path.join(self.base_path, filename)
        data = pd.read_csv(path, header=0).dropna(how='all')
        data['datex'] = pd.to_datetime(data['datex'], errors='coerce')
        data = data.set_index(data['datex']).drop(columns=['datex'])
        data = data.sort_index()
        # print(data)
        return data
        
        
class FeedCostBasics:
    def __init__(self):
        
        self.data = MilkBasics().data 

        self.DR = DateRange()
        self.SD = StatusData()
        self.feed_type =  ['corn','cassava','beans','straw'] 
        
        #DataLoader is the class above
        self.data_loader = DataLoader('F:/COWS/data/feed_data/feed_csv')
        
        self.rng_monthly    = self.DR.date_range_monthly
        self.rng_monthly2   = self.DR.date_range_monthly2
        self.rng_daily      = self.DR.date_range_daily
        
                
        self.herd          = self.create_monthly_alive()
        
        self.price_seq_dict, self.daily_amt_dict    = self.create_feed_cost()   
        self.feed_series_dict      = self.create_separate_feed_series()
        
        self.cost_dict_A          = self.create_cost_A()
        self.cost_dict_B          = self.create_cost_B()
        self.cost_dict_D          = self.create_cost_D()
        
        self.totalcostA, self.totalcost_A_df    = self.create_total_cost_A()
        self.totalcostB, self.totalcost_B_df    = self.create_total_cost_B()
        self.totalcostD, self.totalcost_D_df    = self.create_total_cost_D()
        
        self.feedcostByGroup                    = self.create_milkers_feedcost()
         
        self.milk_income_dash_vars = self.get_dash_vars()
        
    def create_monthly_alive(self):

        self.herd    = self.SD.herd_monthly
        self.herd['dry_15pct']  = (self.herd['milkers count'] * .15).to_frame(name = 'dry 15pct')    
        self.herd['dry_85pct']  = (self.herd['milkers count'] * .85).to_frame(name = 'dry 85pct')
        return   self.herd


    def create_feed_cost(self):
        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        
        for feed in self.feed_type:
            price_seq = self.data_loader.load_csv(f'{feed}_price_seq.csv')
            daily_amt = self.data_loader.load_csv(f'{feed}_daily_amt.csv')
            
            price_seq = price_seq.reindex(self.rng_daily, method='ffill')
            daily_amt = daily_amt.reindex(self.rng_daily, method='ffill')
            
            # Extract year and month from datex column
            price_seq['year'] = price_seq.index.year
            price_seq['month'] = price_seq.index.month
            daily_amt['year'] = daily_amt.index.year
            daily_amt['month'] = daily_amt.index.month
            
              # Set MultiIndex using year and month columns
            price_seq = price_seq.set_index(['year', 'month'])
            daily_amt = daily_amt.set_index(['year', 'month'])
            
            # Group by year and month and calculate the mean
            price_seq = price_seq.groupby(['year', 'month']).agg({
                'unit_price' : 'mean'
            })
            daily_amt = daily_amt.groupby(['year', 'month']).agg({
                'group_a_kg' : 'mean',
                'group_b_kg' : 'mean',
                'dry_kg'    : 'mean'
            })
            
            price_seq = price_seq.reindex(self.rng_monthly, method='ffill')
            daily_amt = daily_amt.reindex(self.rng_monthly, method='ffill')
            
            self.price_seq_dict[feed] = price_seq
            self.daily_amt_dict[feed] = daily_amt
            
        return self.price_seq_dict, self.daily_amt_dict
    
    def create_separate_feed_series(self):
        psd = self.price_seq_dict
        dad = self.daily_amt_dict
        
        self.feed_series_dict = {
            feed: {
                'psd': psd[feed],
                'dad': dad[feed]
                } for feed in self.feed_type
            }
        
        return self.feed_series_dict
            
    
    def create_cost_A(self):
        self.cost_dict_A = {feed: [] for feed in self.feed_type}
        
        for i in self.rng_monthly:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                group_a_kg = self.daily_amt_dict[feed].loc[i, 'group_a_kg']
                cost_A = unit_price * group_a_kg
                self.cost_dict_A[feed].append(cost_A)
        
        return self.cost_dict_A
        
         
    def create_cost_B(self):
        self.cost_dict_B = { feed: [] for feed in self.feed_type }
        
        for i in self.rng_monthly:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                group_b_kg = self.daily_amt_dict[feed].loc[i, 'group_b_kg']
                cost_B = unit_price * group_b_kg
                self.cost_dict_B[feed].append(cost_B)

        return self.cost_dict_B
        
      
    def create_cost_D(self):

        self.cost_dict_D = {feed: [] for feed in self.feed_type}
        
        for i in self.rng_monthly:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                dry_kg = self.daily_amt_dict[feed].loc[i, 'dry_kg']
                cost_D = unit_price * dry_kg
                self.cost_dict_D[feed].append(cost_D)
        
        return self.cost_dict_D
    
    
    def create_total_cost_A(self):

        corn_series     = pd.Series(self.cost_dict_A['corn']).astype(float)
        cassava_series  = pd.Series(self.cost_dict_A['cassava']).astype(float)
        beans_series    = pd.Series(self.cost_dict_A['beans']).astype(float)
        straw_series    = pd.Series(self.cost_dict_A['straw']).astype(float)
        
        totalcostA = corn_series + cassava_series+ beans_series + straw_series
            

        df = pd.DataFrame({
            'corn': corn_series,
            'cassava': cassava_series,
            'beans': beans_series,
            'straw': straw_series,
            'totalcostA': totalcostA
        })
        
        df.index = self.rng_monthly
        df.index.names = ['year', 'month']
        
        self.totalcost_A_df = df  

        return totalcostA, self.totalcost_A_df
                
        
    def create_total_cost_B(self):

        corn_series = pd.Series(self.cost_dict_B['corn']).astype(float)
        cassava_series = pd.Series(self.cost_dict_B['cassava']).astype(float)
        beans_series = pd.Series(self.cost_dict_B['beans']).astype(float)
        straw_series = pd.Series(self.cost_dict_B['straw']).astype(float)
        
        self.totalcostB = corn_series + cassava_series+ beans_series + straw_series
            
        df = pd.DataFrame({
                'corn': corn_series,
                'cassava': cassava_series,
                'beans': beans_series,
                'straw': straw_series,
                'totalcostB': self.totalcostB
            })
                
        df.index = self.rng_monthly
        df.index.names = ['year', 'month']   
        self.totalcost_B_df = df      
           
        return self.totalcostB, self.totalcost_B_df
    
        
    def create_total_cost_D(self):
    
        corn_series = pd.Series(self.cost_dict_D['corn']).astype(float)
        cassava_series = pd.Series(self.cost_dict_D['cassava']).astype(float)
        beans_series = pd.Series(self.cost_dict_D['beans']).astype(float)
        straw_series = pd.Series(self.cost_dict_D['straw']).astype(float)
        
        self.totalcostD = corn_series + cassava_series+ beans_series + straw_series
        
        df = pd.DataFrame({
                'corn': corn_series,
                'cassava': cassava_series,
                'beans': beans_series,
                'straw': straw_series,
                'totalcostD': self.totalcostD
            })
                
        df.index = self.rng_monthly
        df.index.names = ['year', 'month']  
        self.totalcost_D_df = df          
           
        return self.totalcostD, self.totalcost_D_df
            

    def create_milkers_feedcost(self):
        
        feedcost1a  = pd.DataFrame(self.totalcost_A_df  ['totalcostA'])
        feedcost1b  = pd.DataFrame(self.totalcost_B_df  ['totalcostB'])
        feeccost1d  = pd.DataFrame(self.totalcost_D_df  ['totalcostD'])
        feedcost1   = pd.concat((feedcost1a, feedcost1b, feeccost1d), axis=1)
        
        feedcost2 = feedcost1.merge(self.SD.herd_monthly,
                                                  how='outer',
                                                  left_index=True,
                                                  right_index=True)
        
        feedcost2['milkers agg cost'] = feedcost2['milkers count'] * feedcost2['totalcostA']
        feedcost2['dry 15pct agg cost'] = feedcost2['dry_15pct'] * feedcost2['totalcostD']
        feedcost2['dry_agg_cost'] = feedcost2['dry count'] * feedcost2['totalcostD']
        feedcost4 = feedcost2.drop(columns=['total','alive count']) 
        feedcost5 = feedcost4.rename(columns={'totalcostA' : 'cost A', 'totalcostB' : 'cost B', 'totalcostD' : 'dry cost'})
        feedcost6 = feedcost5.reindex(self.rng_monthly2)
        
        self.feedcostByGroup = pd.DataFrame(feedcost6)
        
        return  self.feedcostByGroup
    
    # def calc_total_feedcost(self):
    #     tfc1 = self.feedcostByGroup
    #     tfc2['tfc A'] = tfc1['cost A'] * tfc1['']
                     
    
    def get_dash_vars(self):
        self.milk_income_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.milk_income_dash_vars  


if __name__ == "__main__":
    FBB = FeedCostBasics()
    
                 