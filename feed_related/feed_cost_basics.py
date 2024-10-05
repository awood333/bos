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

    def load_csv(self, filename):
        path = os.path.join(self.base_path, filename)
        data = pd.read_csv(path, header=0).dropna(how='all')
        data['datex'] = pd.to_datetime(data['datex'])
        data = data.set_index(data['datex']).drop(columns=['datex'])
        return data
        
        
class FeedCostBasics:
    def __init__(self):

        self.DR = DateRange()
        self.SD = StatusData()
        self.feed_type =  ['corn','cassava','beans','straw'] 
        self.data_loader = DataLoader('F:/COWS/data/feed_data/feed_csv')
                
        [self.alive3, self.milkers3, 
         self.dry3, self.dry3_15pct , self.dry3_85pct, 
         self.rng_monthly]               = self.create_monthly_alive()
        
        self.price_seq_dict, self.daily_amt_dict    = self.create_feed_cost()   
        self.feed_series_dict      = self.create_separate_feed_series()
        
        self.cost_dict_A          = self.create_cost_A()
        self.cost_dict_B          = self.create_cost_B()
        self.cost_dict_D          = self.create_cost_D()
        
        self.totalcostA, self.totalcost_A_df    = self.create_total_cost_A()
        self.totalcostB, self.totalcost_B_df    = self.create_total_cost_B()
        self.totalcostD, self.totalcost_D_df    = self.create_total_cost_D()
        
        self.feedcostByGroup                    = self.create_milkers_feedcost()
        
        self.tfc           = self.lactation_average_food_cost()
        self.gtc           = self.create_lactation_average_total_cost()
        self.milk_income_dash_vars = self.get_dash_vars()
        
    def create_monthly_alive(self):

        self.rng_monthly    = self.DR.date_range_monthly
        self.rng_daily      = self.DR.date_range_daily
        
        herd_df             = self.SD.herd_df 
               
        herd_df['year']   = herd_df.index.year
        herd_df['month']  = herd_df.index.month
        herd_df['days']   = herd_df.index.days_in_month
        
        alive_df            = herd_df['alive'].to_frame()
        milkers_df          = herd_df['milkers'].to_frame()
        dry_df              = herd_df['dry'].to_frame()
                
        dfs = {
            'alive': alive_df,
            'milkers': milkers_df,
            'dry': dry_df
        }

        results = {}

        for key, df in dfs.items():
            df['year'] = df.index.year
            df['month'] = df.index.month
            df['days'] = df.index.days_in_month
            
            grouped_df = df.groupby(['year', 'month']).agg({
                key: 'mean',
                'days': 'first'
            })
            
            results[key] = grouped_df

        self.alive3 = results['alive']
        self.milkers3 = results['milkers']
        self.dry3 = results['dry']
        
        self.dry3_15pct = (self.milkers3['milkers'] * .15).to_frame(name = 'dry 15pct')    
        self.dry3_85pct  = (self.milkers3['milkers'] * .85).to_frame(name = 'dry 85%')   
        
        
        return self.alive3, self.milkers3, self.dry3, self.dry3_15pct , self.dry3_85pct, self.rng_monthly


    def create_feed_cost(self):
        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        
        for feed in self.feed_type:
            price_seq = self.data_loader.load_csv(f'{feed}_price_seq.csv')
            daily_amt = self.data_loader.load_csv(f'{feed}_daily_amt.csv')
            
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
        
        for i in self.rng_monthly:
            corn_series = pd.Series(self.cost_dict_A['corn']).astype(float)
            cassava_series = pd.Series(self.cost_dict_A['cassava']).astype(float)
            beans_series = pd.Series(self.cost_dict_A['beans']).astype(float)
            straw_series = pd.Series(self.cost_dict_A['straw']).astype(float)
            
            self.totalcostA = corn_series + cassava_series+ beans_series + straw_series

        df = pd.DataFrame({
                'corn': corn_series,
                'cassava': cassava_series,
                'beans': beans_series,
                'straw': straw_series,
                'totalcostA': self.totalcostA
            })
        
        df.index = self.alive3.index

        self.totalcost_A_df = df  

        return self.totalcostA,  self.totalcost_A_df
            
        
    def create_total_cost_B(self):

        
        for i in self.rng_monthly:
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
                
        df.index = self.alive3.index
        self.totalcost_B_df = df            
           
        return self.totalcostB, self.totalcost_B_df
    
        
    def create_total_cost_D(self):
        
        for i in self.rng_monthly:
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
                
        df.index = self.alive3.index
        
        self.totalcost_D_df = df            
           
        return self.totalcostD, self.totalcost_D_df
            

    def create_milkers_feedcost(self):
        
        milkersfeedcost1 = pd.DataFrame(self.totalcost_A_df['totalcostA'])
        milkersfeedcost2 = milkersfeedcost1.merge(self.SD.herd_monthly,
                                                  how='outer',
                                                  left_index=True,
                                                  right_index=True)
        
        # milkersfeedcost2 = milkersfeedcost2a.drop(columns='dry')
        milkersfeedcost2['dry 15pct']  = self.dry3_15pct['dry 15pct']
        
        
        drycost = pd.DataFrame(self.totalcost_D_df['totalcostD'])
        milkersfeedcost3 = milkersfeedcost2.merge(drycost,
                                                  how='outer',
                                                  left_index=True,
                                                  right_index=True)
        
        milkersfeedcost3['milkers agg cost'] = milkersfeedcost3['milkers'] * milkersfeedcost3['totalcostA']
        milkersfeedcost3['dry 15pct agg cost'] = milkersfeedcost3['dry 15pct'] * milkersfeedcost3['totalcostD']
        milkersfeedcost3['dry_agg_cost'] = milkersfeedcost3['dry'] * milkersfeedcost3['totalcostD']
        milkersfeedcost4 = milkersfeedcost3.drop(columns=['gone','total','alive'])
        
        totalcostD = milkersfeedcost4.pop('totalcostD')
        milkersfeedcost4.insert(1, 'totalcostD', totalcostD)
        
        milkersfeedcost5 = milkersfeedcost4.rename(columns={'totalcostA' : 'milkers cost', 'totalcostD' : 'dry cost'})
        self.feedcostByGroup = milkersfeedcost5
        
        return  self.feedcostByGroup
                     
        
    def lactation_average_food_cost(self):
        
        # Note this is based on a 52 week lactation
        tca = pd.Series(self.totalcostA).astype(float) * 22
        tcb = pd.Series(self.totalcostB).astype(float) * 22
        tcd = pd.Series(self.totalcostD).astype(float) * 8
        tfc1 = tca + tcb + tcd
        self.tfc  = tfc1 / 52
        return self.tfc
    
    def create_lactation_average_total_cost(self):
        self.tfc.index += 1
        self.tfc.index = self.alive3.index
        self.gtc = self.tfc * self.alive3['days'] * self.alive3['alive']

        return self.gtc
    
    def get_dash_vars(self):
        self.milk_income_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.milk_income_dash_vars  


if __name__ == "__main__":
    FBB = FeedCostBasics()
    
                 