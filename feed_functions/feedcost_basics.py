'''feed_functions\\feedcost_basics.py'''

import os       #don't erase
# from datetime import datetime
import pandas as pd

from CreateStartDate import DateRange
from status_functions.statusData import StatusData
from MilkBasics import MilkBasics 

# from utilities.logging_setup import LoggingSetup

class DataLoader:
    def __init__(self,base_path):

        self.base_path = base_path

    def load_csv(self, filename):
        path = os.path.join(self.base_path, filename)
        data = pd.read_csv(path, header=0).dropna(how='all')
        data['datex'] = pd.to_datetime(data['datex'], errors='coerce')
        data = data.set_index(data['datex']).drop(columns=['datex'])
        data = data.sort_index()
        return data
        
class Feedcost_basics:
    def __init__(self, milk_basics=None, date_range=None, status_data=None):
        self.data_loader = DataLoader('F:/COWS/data/feed_data/feed_csv')
       
        self.data = milk_basics or MilkBasics().data
        self.DR = date_range or DateRange()
        self.SD = status_data or StatusData()
        self.feed_type =  ['corn','cassava','beans','straw', 'bypass_fat']

        self.rng_monthly    = self.DR.date_range_monthly
        self.rng_monthly2   = self.DR.date_range_monthly2
        self.rng_daily      = self.DR.date_range_daily

        [self.last_values_all_df, 
         self.current_feedcost]         = self.create_feed_cost()
        
        self.feed_series_dict           = self.create_separate_feed_series()
        
        [self.last_values_all_df, 
         self.current_feedcost, 
         self.unit_prices_daily]        =  self.create_last_values()
        
        [self.cost_dict_A, 
         self.last_cost_details_A_df]   = self.create_cost_A()
        
        self.cost_dict_B                = self.create_cost_B()
        self.cost_dict_D                = self.create_cost_D()
        
        self.totalcostA, self.totalcost_A_df    = self.create_total_cost_A()
        self.totalcostB, self.totalcost_B_df    = self.create_total_cost_B()
        self.totalcostD, self.totalcost_D_df    = self.create_total_cost_D()
        
        self.feedcost_daily, self.feedcost_monthly       = self.create_feedcostByGroup()
        self.write_to_csv()


    def create_feed_cost(self):
        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        
        for feed in self.feed_type:
            price_seq = self.data_loader.load_csv(f'{feed}_price_seq.csv')
            daily_amt = self.data_loader.load_csv(f'{feed}_daily_amt.csv')
            
            price_seq = price_seq.reindex(self.rng_daily, method='ffill')
            daily_amt = daily_amt.reindex(self.rng_daily, method='ffill')
            
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

       
       
    #    this method makes a simple table of the current cost values
    def create_last_values(self):
        
        last_values_all = {
            feed: {
                'psd': self.feed_series_dict[feed]['psd'].iloc[-1],
                'dad': self.feed_series_dict[feed]['dad'].iloc[-1],
            } for feed in self.feed_type
            }
        
        self.last_values_all_df = pd.DataFrame.from_dict(last_values_all, orient='index')
        
        unit_prices, group_a_kg, group_b_kg, dry_kg  = {},{},{},{}
        
        for feed in self.feed_type:
            unit_prices[feed]       = self.last_values_all_df.loc[feed, 'psd'] ['unit_price']
            
        for feed in self.feed_type:
            group_a_kg[feed]        = self.last_values_all_df.loc[feed, 'dad']['group_a_kg']
            
        for feed in self.feed_type:
            group_b_kg[feed]        = self.last_values_all_df.loc[feed, 'dad']['group_b_kg']
            
        for feed in self.feed_type:
            dry_kg[feed]            = self.last_values_all_df.loc[feed, 'dad']['dry_kg']            
                
        # Convert dictionaries to DataFrames
        unit_prices_df= pd.DataFrame.from_dict(unit_prices, orient='index', columns=['unit_price'])
        group_a_kg_df = pd.DataFrame.from_dict(group_a_kg,  orient='index', columns=['group_a_kg'])
        group_b_kg_df = pd.DataFrame.from_dict(group_b_kg,  orient='index', columns=['group_b_kg'])
        dry_kg_df =     pd.DataFrame.from_dict(dry_kg,      orient='index', columns=['dry_kg'])

        # Concatenate DataFrames
        result_df = pd.concat([unit_prices_df, group_a_kg_df, group_b_kg_df, dry_kg_df], axis=1)
        result_df['group_a_cost']   = result_df['unit_price'] * result_df['group_a_kg']
        result_df['group_b_cost']   = result_df['unit_price'] * result_df['group_b_kg']
        result_df['dry_cost']       = result_df['unit_price'] * result_df['dry_kg']
        
        sum_row = result_df[['group_a_cost', 'group_b_cost', 'dry_cost',
                             'group_a_kg', 'group_b_kg', 'dry_kg' ]].sum(axis=0)
        sum_row.name = 'sum'
        sum_row['unit_price'] = ''
        # sum_row['group_a_kg'] = ''
        # sum_row['group_b_kg'] = ''
        # sum_row['dry_kg'] = ''
        
        sum_row_df = pd.DataFrame(sum_row).T
        result_df = pd.concat([result_df, sum_row_df], ignore_index=False)
        self.current_feedcost = result_df
        self.unit_prices_daily = unit_prices_df

        
        return self.last_values_all_df, self.current_feedcost, self.unit_prices_daily
    
    
    
    def create_cost_A(self):
        self.cost_dict_A = {feed: [] for feed in self.feed_type}
        
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                group_a_kg = self.daily_amt_dict[feed].loc[i, 'group_a_kg']
                cost_A = unit_price * group_a_kg
                self.cost_dict_A[feed].append(cost_A)
                
        last_cost_details_A = {
            key: value[-1] for key, value in self.cost_dict_A.items()
            }
            
        self.last_cost_details_A_df = pd.DataFrame.from_dict(last_cost_details_A, orient='index')
        
        return self.cost_dict_A, self.last_cost_details_A_df 
        
         
    def create_cost_B(self):
        self.cost_dict_B = { feed: [] for feed in self.feed_type }
        
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                group_b_kg = self.daily_amt_dict[feed].loc[i, 'group_b_kg']
                cost_B = unit_price * group_b_kg
                self.cost_dict_B[feed].append(cost_B)

        return self.cost_dict_B
        
      
    def create_cost_D(self):

        self.cost_dict_D = {feed: [] for feed in self.feed_type}
        
        for i in self.rng_daily:
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
        
        df.index = self.rng_daily
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
                
        df.index = self.rng_daily
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
                
        df.index = self.rng_daily
        self.totalcost_D_df = df          
           
        return self.totalcostD, self.totalcost_D_df
            

    def create_feedcostByGroup(self):
        
        feedcost1a  = pd.DataFrame(self.totalcost_A_df  ['totalcostA'])
        feedcost1b  = pd.DataFrame(self.totalcost_B_df  ['totalcostB'])
        feeccost1d  = pd.DataFrame(self.totalcost_D_df  ['totalcostD'])
        feedcost_daily1   = pd.concat((feedcost1a, feedcost1b, feeccost1d), axis=1)
        
        feedcost_monthly1 = pd.DataFrame(feedcost_daily1)
        feedcost_monthly1['year']    = feedcost_daily1.index.year
        feedcost_monthly1['month'] = feedcost_daily1.index.month
        feedcost_monthly1['days']  = feedcost_daily1.index.days_in_month    

        self.feedcost_daily = feedcost_daily1
        self.feedcost_monthly  = feedcost_monthly1.groupby(['year','month', 'days']). agg('mean')
        
        return  self.feedcost_daily, self.feedcost_monthly
    
    def write_to_csv(self):
        
        self.current_feedcost   .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\current_feedcost_per_cow.csv')        
        self.feedcost_daily     .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup__per_cow_daily.csv')
        self.feedcost_monthly   .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup__per_cow_monthly.csv')
        
    
    

if __name__ == "__main__":
    Feedcost_basics()
    
                 