'''feed_related\\feed_cost_basics.py'''

import os       #don't erase
from datetime import datetime
import pandas as pd


from feed_related.CreateStartDate import DateRange
from milk_functions.status_ids import StatusData
from utilities.logging_setup import LoggingSetup

class DataLoader:
    def __init__(self,base_path):
        self.base_path = base_path

    def load_csv(self,filename):
        path = os.path.join(self.base_path, filename)
        data = pd.read_csv(path, header=0).dropna
        
        
class FeedCostBasics:
    def __init__(self):
        
        self.DR = DateRange()
        self.SD = StatusData()
        self.feed_type =  ['corn','cassava','beans','straw'] 
        
        self.alive3, self.rng_monthly               = self.create_monthly_alive()

        self.price_seq_dict, self.daily_amt_dict    = self.create_feed_cost()
        
        [
        self.corn_psd,
        self.cassava_psd,
        self.beans_psd,
        self.straw_psd, 
        
        self.corn_dad,
        self.cassava_dad,
        self.beans_dad,
        self.straw_dad
        ]                       = self.create_separate_feed_series()
        
        self.cost_dict_A          = self.create_cost_A()
        self.cost_dict_B          = self.create_cost_B()
        self.cost_dict_D          = self.create_cost_D()
        
        self.totalcostA    = self.create_total_cost_A()
        self.totalcostB    = self.create_total_cost_B()
        self.totalcostD    = self.create_total_cost_D()
        
        self.tfc           = self.create_total_food_cost()
        self.gtc           = self.create_grand_total_cost()
        
    def create_monthly_alive(self):

        self.rng_monthly    = self.DR.date_range_monthly
        self.rng_daily      = self.DR.date_range_daily
        # herd_trunc          = self.SD.herd.reindex(rng_)
        alive_df            = self.SD.herd_df['alive'].to_frame()
        alive_df['yearx']   = alive_df.index.year
        alive_df['monthx']  = alive_df.index.month
        alive_df['days']    = alive_df.index.days_in_month
        # alive_df.reindex(self.rng_daily)

        
        alive2 = alive_df.groupby(['yearx','monthx']).agg({
            'alive' : 'mean',
            'days'  : 'first'
            } )  #.reset_index()
        
        self.alive3 = alive2
        return self.alive3, self.rng_monthly


    def create_feed_cost(self):
         
        base_path = 'F:/COWS/data/feed_data/feed_csv'
        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        
        for feed in self.feed_type:        
            price_seq_path = os.path.join(base_path, f'{feed}_price_seq.csv')
            daily_amt_path = os.path.join(base_path, f'{feed}_daily_amt.csv')

            price_seq   = pd.read_csv(price_seq_path,  header=0).dropna(how='all')  
            daily_amt   = pd.read_csv(daily_amt_path,  header=0).dropna(how='all')  
            
            price_seq['datex'] = pd.to_datetime(price_seq['datex'])
            daily_amt['datex'] = pd.to_datetime(daily_amt['datex'])
            
            price_seq = price_seq.set_index(price_seq['datex']) .drop(columns=['datex']) 
            daily_amt = daily_amt.set_index(daily_amt['datex']) .drop(columns=['datex']) 
                             
            price_seq = price_seq.reindex(self.rng_monthly, method='ffill')
            daily_amt = daily_amt.reindex(self.rng_monthly, method='ffill')
            
            self.price_seq_dict[feed] = price_seq
            self.daily_amt_dict[feed] = daily_amt
            
        return self.price_seq_dict, self.daily_amt_dict
    
    
    def create_separate_feed_series (self):
        
        psd = self.price_seq_dict
        dad = self.daily_amt_dict
        
        self.corn_psd       = psd['corn']
        self.cassava_psd    = psd['cassava']
        self.beans_psd      = psd['beans']
        self.straw_psd      = psd['straw'] 
        
        self.corn_dad       = dad['corn']
        self.cassava_dad    = dad['cassava']
        self.beans_dad      = dad['beans']
        self.straw_dad      = dad['straw']
        
        return  [
        self.corn_psd,
        self.cassava_psd,
        self.beans_psd,
        self.straw_psd, 
        
        self.corn_dad,
        self.cassava_dad,
        self.beans_dad,
        self.straw_dad
        ]
        
    def create_cost_A(self):
        self.cost_dict_A = {
            'corn': [],
            'cassava': [],
            'beans': [],
            'straw': []
        }
        
        for i in self.rng_monthly:
            corn_cost_A     = self.corn_psd.loc[i,'unit_price']      * self.corn_dad.loc[i,'group_a_kg']
            cassava_cost_A  = self.cassava_psd.loc[i, 'unit_price']  * self.cassava_dad.loc[i, 'group_a_kg']
            beans_cost_A    = self.beans_psd.loc[i, 'unit_price']    * self.beans_dad.loc[i, 'group_a_kg']
            straw_cost_A    = self.straw_psd.loc[i, 'unit_price']    * self.straw_dad.loc[i, 'group_a_kg']
            
            self.cost_dict_A['corn'].append(corn_cost_A)
            self.cost_dict_A['cassava'].append(cassava_cost_A)
            self.cost_dict_A['beans'].append(beans_cost_A)
            self.cost_dict_A['straw'].append(straw_cost_A)
        
        return self.cost_dict_A
            
    def create_cost_B(self):
        self.cost_dict_B = {
            'corn': [],
            'cassava': [],
            'beans': [],
            'straw': []
        }
        
        for i in self.rng_monthly:
            corn_cost_B     = self.corn_psd.loc[i,'unit_price']      * self.corn_dad.loc    [i, 'group_b_kg']
            cassava_cost_B  = self.cassava_psd.loc[i, 'unit_price']  * self.cassava_dad.loc [i, 'group_b_kg']
            beans_cost_B    = self.beans_psd.loc[i, 'unit_price']    * self.beans_dad.loc   [i, 'group_b_kg']
            straw_cost_B    = self.straw_psd.loc[i, 'unit_price']    * self.straw_dad.loc   [i, 'group_b_kg']
            
            self.cost_dict_B['corn'].append(corn_cost_B)
            self.cost_dict_B['cassava'].append(cassava_cost_B)
            self.cost_dict_B['beans'].append(beans_cost_B)
            self.cost_dict_B['straw'].append(straw_cost_B)
        
        return self.cost_dict_B
        
        
    def create_cost_D(self):
        self.cost_dict_D = {
            'corn': [],
            'cassava': [],
            'beans': [],
            'straw': []
        }
        
        for i in self.rng_monthly:
            corn_cost_D     = self.corn_psd.loc     [i,'unit_price']      * self.corn_dad.loc   [i, 'dry_kg']
            cassava_cost_D  = self.cassava_psd.loc  [i, 'unit_price']  * self.cassava_dad.loc   [i, 'dry_kg']
            beans_cost_D    = self.beans_psd.loc    [i, 'unit_price']    * self.beans_dad.loc   [i, 'dry_kg']
            straw_cost_D    = self.straw_psd.loc    [i, 'unit_price']    * self.straw_dad.loc   [i, 'dry_kg']
            
            self.cost_dict_D['corn']    .append(corn_cost_D)
            self.cost_dict_D['cassava'] .append(cassava_cost_D)
            self.cost_dict_D['beans']   .append(beans_cost_D)
            self.cost_dict_D['straw']   .append(straw_cost_D)
        
        return self.cost_dict_D
    
    def create_total_cost_A(self):
        self.total_A, self.sum_A = [],[]
        
        for i in self.rng_monthly:
            corn_series = pd.Series(self.cost_dict_A['corn']).astype(float)
            cassava_series = pd.Series(self.cost_dict_A['cassava']).astype(float)
            beans_series = pd.Series(self.cost_dict_A['beans']).astype(float)
            straw_series = pd.Series(self.cost_dict_A['straw']).astype(float)
            
            self.totalcostA = corn_series + cassava_series+ beans_series + straw_series
    
        return self.totalcostA
            
        
    def create_total_cost_B(self):
        self.total_B = []
        self.sum_B = []
        
        for i in self.rng_monthly:
            corn_series = pd.Series(self.cost_dict_B['corn']).astype(float)
            cassava_series = pd.Series(self.cost_dict_B['cassava']).astype(float)
            beans_series = pd.Series(self.cost_dict_B['beans']).astype(float)
            straw_series = pd.Series(self.cost_dict_B['straw']).astype(float)
            
            self.totalcostB = corn_series + cassava_series+ beans_series + straw_series
           
        return self.totalcostB
    
        
    def create_total_cost_D(self):
        total_D = []
        sum_D = []
        
        for i in self.rng_monthly:
            corn_series = pd.Series(self.cost_dict_D['corn']).astype(float)
            cassava_series = pd.Series(self.cost_dict_D['cassava']).astype(float)
            beans_series = pd.Series(self.cost_dict_D['beans']).astype(float)
            straw_series = pd.Series(self.cost_dict_D['straw']).astype(float)
            
            self.totalcostD = corn_series + cassava_series + beans_series + straw_series
        return  self.totalcostD
            
        
    def create_total_food_cost(self):
        tca = pd.Series(self.totalcostA).astype(float) * 22
        tcb = pd.Series(self.totalcostB).astype(float) * 22
        tcd = pd.Series(self.totalcostD).astype(float) * 8
        tfc1 = tca + tcb + tcd
        self.tfc  = tfc1 / 52
        return self.tfc
    
    def create_grand_total_cost(self):
        self.tfc.index += 1
        self.tfc.index = self.alive3.index
        print('tfc',self.tfc, '\n', 'alive3 ', self.alive3)
        self.gtc = self.tfc * self.alive3['days'] * self.alive3['alive']
        print(self.gtc)
        return self.gtc


if __name__ == "__main__":
    FBB = FeedCostBasics()
    
                 