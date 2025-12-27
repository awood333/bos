'''feed_functions\\feedcost_basics.py'''
import inspect
import os       #don't erase
import pandas as pd

from container import get_dependency

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
    def __init__(self):
        print(f"Feedcost_basics instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.data_loader = None
        self.feed_type = ['corn','cassava','beans','straw', 'bypass_fat', 'CP_005_21P', 'NaHCO3', 'CP_milk2']
        self.rng_monthly = None
        self.rng_monthly2 = None
        self.rng_daily = None

        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        self.feed_series_dict = {}
        self.last_values_all_df = None
        self.current_feedcost = None
        self.unit_prices_daily = None
        self.cost_dict_F = {}
        self.last_cost_details_F_df = None        
        self.cost_dict_A = {}
        self.last_cost_details_A_df = None
        self.cost_dict_B = {}
        self.last_cost_details_B_df = None
        self.cost_dict_C = {}
        self.last_cost_details_C_df = None
        self.cost_dict_D = {}
        self.last_cost_details_D_df = None
        self.totalcost_A_df = None
        self.totalcost_B_df = None
        self.totalcost_C_df = None
        self.totalcost_D_df = None
        self.totalcost_F_df = None
        self.feedcost_daily = None
        self.feedcost_weekly = None            
        self.feedcost_monthly = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.data_loader = DataLoader('F:/COWS/data/feed_data/feed_csv')

        self.rng_monthly  = self.DR.date_range_monthly
        self.rng_monthly2 = getattr(self.DR, 'date_range_monthly2', None)
        self.rng_daily    = self.DR.date_range_daily

        self.price_seq_dict, self.daily_amt_dict = self.create_feed_cost_dict()
        self.feed_series_dict = self.create_separate_feed_series()
        self.last_values_all_df, self.current_feedcost, self.unit_prices_daily = self.create_last_values()
        self.cost_dict_F, self.last_cost_details_F_df = self.create_cost_F()        
        self.cost_dict_A, self.last_cost_details_A_df = self.create_cost_A()
        self.cost_dict_B, self.last_cost_details_B_df = self.create_cost_B()
        self.cost_dict_C, self.last_cost_details_C_df = self.create_cost_C()
        self.cost_dict_D, self.last_cost_details_D_df = self.create_cost_D()
        self.totalcost_F_df = self.create_total_cost_F()        
        self.totalcost_A_df = self.create_total_cost_A()
        self.totalcost_B_df = self.create_total_cost_B()
        self.totalcost_C_df = self.create_total_cost_C()
        self.totalcost_D_df = self.create_total_cost_D()
        self.feedcost_daily, self.feedcost_monthly, self.feedcost_weekly = self.create_total_feedcostByGroup()
        self.write_to_csv()

    def create_feed_cost_dict(self):
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
     
    def create_last_values(self):
    #    this method makes a simple table of the current cost values
        
        last_values_all = {
            feed: {
                'psd': self.feed_series_dict[feed]['psd'].iloc[-1],
                'dad': self.feed_series_dict[feed]['dad'].iloc[-1],
            } for feed in self.feed_type
            }


        self.last_values_all_df = pd.DataFrame.from_dict(last_values_all, orient='index')
        
        unit_prices, group_a_kg, group_b_kg, group_c_kg, dry_kg  = {},{},{},{},{}
        
        for feed in self.feed_type:
            unit_prices[feed]       = self.last_values_all_df.loc[feed, 'psd'] ['unit_price']
            
        for feed in self.feed_type:
            group_a_kg[feed]        = self.last_values_all_df.loc[feed, 'dad']['group_a_kg']
            
        for feed in self.feed_type:
            group_b_kg[feed]        = self.last_values_all_df.loc[feed, 'dad']['group_b_kg']

        for feed in self.feed_type:
            group_c_kg[feed]        = self.last_values_all_df.loc[feed, 'dad']['group_c_kg']
            
        for feed in self.feed_type:
            dry_kg[feed]            = self.last_values_all_df.loc[feed, 'dad']['dry_kg']            
                
        # Convert dictionaries to DataFrames
        unit_prices_df= pd.DataFrame.from_dict(unit_prices, orient='index', columns=['unit_price'])
        group_a_kg_df = pd.DataFrame.from_dict(group_a_kg,  orient='index', columns=['group_a_kg'])
        group_b_kg_df = pd.DataFrame.from_dict(group_b_kg,  orient='index', columns=['group_b_kg'])
        group_c_kg_df = pd.DataFrame.from_dict(group_c_kg,  orient='index', columns=['group_c_kg'])        
        dry_kg_df =     pd.DataFrame.from_dict(dry_kg,      orient='index', columns=['dry_kg'])

        # Concatenate DataFrames
        result_df = pd.concat([unit_prices_df, group_a_kg_df, group_b_kg_df, group_c_kg_df, dry_kg_df], axis=1)
        result_df['group_a_cost']   = result_df['unit_price'] * result_df['group_a_kg']
        result_df['group_b_cost']   = result_df['unit_price'] * result_df['group_b_kg']
        result_df['group_c_cost']   = result_df['unit_price'] * result_df['group_c_kg']
        result_df['dry_cost']       = result_df['unit_price'] * result_df['dry_kg']
        
        sum_row = result_df[['group_a_cost', 'group_b_cost', 'group_c_cost', 'dry_cost',
                             'group_a_kg', 'group_b_kg', 'group_c_kg', 'dry_kg' ]].sum(axis=0)
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
    
    def create_cost_F(self):

        # NOTE: if you add a feed type, make sure it gets accessed in ALL the methods below 
        self.cost_dict_F = {feed: [] for feed in self.feed_type}
        
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                fresh_kg = self.daily_amt_dict[feed].loc[i, 'fresh_kg']
                cost_F = unit_price * fresh_kg
                self.cost_dict_F[feed].append(cost_F)
                
        last_cost_details_F = {
            key: value[-1] for key, value in self.cost_dict_F.items()
            }
            
        self.last_cost_details_F_df = pd.DataFrame.from_dict(last_cost_details_F, orient='index')
        
        return self.cost_dict_F, self.last_cost_details_F_df 


    def create_cost_A(self):

        # NOTE: if you add a feed type, make sure it gets accessed in ALL the methods below 
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
        
        last_cost_details_B = {
            key: value[-1] for key, value in self.cost_dict_B.items()
            }
            
        self.last_cost_details_B_df = pd.DataFrame.from_dict(last_cost_details_B, orient='index')

        return self.cost_dict_B, self.last_cost_details_B_df 
    
    def create_cost_C(self):
        self.cost_dict_C = { feed: [] for feed in self.feed_type }
        
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                group_c_kg = self.daily_amt_dict[feed].loc[i, 'group_c_kg']
                cost_C = unit_price * group_c_kg
                self.cost_dict_C[feed].append(cost_C)

                    
        last_cost_details_C = {
            key: value[-1] for key, value in self.cost_dict_C.items()
            }
            
        self.last_cost_details_C_df = pd.DataFrame.from_dict(last_cost_details_C, orient='index')

        return self.cost_dict_C, self.last_cost_details_C_df 
      
    def create_cost_D(self):

        self.cost_dict_D = {feed: [] for feed in self.feed_type}
        
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                dry_kg = self.daily_amt_dict[feed].loc[i, 'dry_kg']
                cost_D = unit_price * dry_kg
                self.cost_dict_D[feed].append(cost_D)

                            
        last_cost_details_D = {
            key: value[-1] for key, value in self.cost_dict_D.items()
            }
            
        self.last_cost_details_D_df = pd.DataFrame.from_dict(last_cost_details_D, orient='index')

        return self.cost_dict_D, self.last_cost_details_D_df 
        
    
    def create_total_cost_F(self):

        corn_series     = pd.Series(self.cost_dict_F['corn']).astype(float)
        cassava_series  = pd.Series(self.cost_dict_F['cassava']).astype(float)
        beans_series    = pd.Series(self.cost_dict_F['beans']).astype(float)
        straw_series    = pd.Series(self.cost_dict_F['straw']).astype(float)
        CP_milk2_series = pd.Series(self.cost_dict_F['CP_milk2']).astype(float)
        CP_005_21P_series   = pd.Series(self.cost_dict_F['CP_005_21P']).astype(float)
        NaHCO3_series       = pd.Series(self.cost_dict_F['NaHCO3']).astype(float)
        bypass_fat_series   = pd.Series(self.cost_dict_F['bypass_fat']).astype(float)

        totalcostF = (corn_series   + cassava_series
                      + beans_series + straw_series 
                      + bypass_fat_series + CP_005_21P_series 
                      + NaHCO3_series + CP_milk2_series
                     ) 
            

        df = pd.DataFrame({
            'corn': corn_series,
            'cassava': cassava_series,
            'beans': beans_series,
            'straw': straw_series,
            'bypass_fat': bypass_fat_series,
            'CP_005_21P': CP_005_21P_series,
            'NaHCO3': NaHCO3_series,
            'CP_milk2' : CP_milk2_series,

            'totalcostF': totalcostF
        })
        
        df.index = self.rng_daily
        self.totalcost_F_df = df

        return self.totalcost_F_df  
    
    def create_total_cost_A(self):

        corn_series     = pd.Series(self.cost_dict_A['corn']).astype(float)
        cassava_series  = pd.Series(self.cost_dict_A['cassava']).astype(float)
        beans_series    = pd.Series(self.cost_dict_A['beans']).astype(float)
        straw_series    = pd.Series(self.cost_dict_A['straw']).astype(float)
        CP_milk2_series    = pd.Series(self.cost_dict_A['CP_milk2']).astype(float)
        CP_005_21P_series   = pd.Series(self.cost_dict_A['CP_005_21P']).astype(float)
        NaHCO3_series   = pd.Series(self.cost_dict_A['NaHCO3']).astype(float)
        bypass_fat_series = pd.Series(self.cost_dict_A['bypass_fat']).astype(float)

        totalcostA = (corn_series   + cassava_series
                      + beans_series + straw_series 
                      + bypass_fat_series + CP_005_21P_series 
                      + NaHCO3_series + CP_milk2_series
                     ) 
            

        df = pd.DataFrame({
            'corn': corn_series,
            'cassava': cassava_series,
            'beans': beans_series,
            'straw': straw_series,
            'bypass_fat': bypass_fat_series,
            'CP_005_21P': CP_005_21P_series,
            'NaHCO3': NaHCO3_series,
            'CP_milk2' : CP_milk2_series,

            'totalcostA': totalcostA
        })
        
        df.index = self.rng_daily
        self.totalcost_A_df = df  

        return self.totalcost_A_df
                
        
    def create_total_cost_B(self):

        corn_series     = pd.Series(self.cost_dict_B['corn']).astype(float)
        cassava_series  = pd.Series(self.cost_dict_B['cassava']).astype(float)
        beans_series    = pd.Series(self.cost_dict_B['beans']).astype(float)
        straw_series    = pd.Series(self.cost_dict_B['straw']).astype(float)
        CP_005_21P_series   = pd.Series(self.cost_dict_B['CP_005_21P']).astype(float)
        CP_milk2_series    = pd.Series(self.cost_dict_B['CP_milk2']).astype(float)
        NaHCO3_series   = pd.Series(self.cost_dict_B['NaHCO3']).astype(float)
        bypass_fat_series    = pd.Series(self.cost_dict_B['bypass_fat']).astype(float)

        totalcostB = (corn_series + cassava_series 
                      + beans_series + straw_series 
                      + bypass_fat_series + CP_005_21P_series 
                      + NaHCO3_series + CP_milk2_series
                      )
        
        df = pd.DataFrame({
            'corn': corn_series,
            'cassava': cassava_series,
            'beans': beans_series,
            'straw': straw_series,
            'bypass_fat': bypass_fat_series,
            'CP_005_21P': CP_005_21P_series,
            'CP_milk2' : CP_milk2_series,
            'NaHCO3': NaHCO3_series,
            
            'totalcostB': totalcostB
        })
                
        df.index = self.rng_daily
        self.totalcost_B_df = df      
           
        return self.totalcost_B_df
    

    def create_total_cost_C(self):

        corn_series     = pd.Series(self.cost_dict_C['corn']).astype(float)
        cassava_series  = pd.Series(self.cost_dict_C['cassava']).astype(float)
        beans_series    = pd.Series(self.cost_dict_C['beans']).astype(float)
        straw_series    = pd.Series(self.cost_dict_C['straw']).astype(float)
        CP_005_21P_series   = pd.Series(self.cost_dict_C['CP_005_21P']).astype(float)
        CP_milk2_series    = pd.Series(self.cost_dict_C['CP_milk2']).astype(float)
        NaHCO3_series   = pd.Series(self.cost_dict_C['NaHCO3']).astype(float)
        bypass_fat_series = pd.Series(self.cost_dict_C['bypass_fat']).astype(float)

        totalcostC = (corn_series + cassava_series 
                      + beans_series + straw_series 
                      + bypass_fat_series + CP_005_21P_series 
                      + NaHCO3_series + CP_milk2_series
                     ) 

        df = pd.DataFrame({
            'corn': corn_series,
            'cassava': cassava_series,
            'beans': beans_series,
            'straw': straw_series,
            'bypass_fat': bypass_fat_series,
            'CP_005_21P': CP_005_21P_series,
            'CP_milk2' : CP_milk2_series,
            'NaHCO3': NaHCO3_series,

            'totalcostC': totalcostC
        })
                
        df.index = self.rng_daily
        self.totalcost_C_df = df      
           
        return self.totalcost_C_df
    
        
    def create_total_cost_D(self):
    
        corn_series = pd.Series(self.cost_dict_D['corn']).astype(float)
        cassava_series = pd.Series(self.cost_dict_D['cassava']).astype(float)
        beans_series = pd.Series(self.cost_dict_D['beans']).astype(float)
        straw_series = pd.Series(self.cost_dict_D['straw']).astype(float)
        CP_005_21P_series    = pd.Series(self.cost_dict_D['CP_005_21P']).astype(float)
        CP_milk2_series    = pd.Series(self.cost_dict_D['CP_milk2']).astype(float)
        NaHCO3_series    = pd.Series(self.cost_dict_D['NaHCO3']).astype(float)
        bypass_fat_series    = pd.Series(self.cost_dict_D['bypass_fat']).astype(float)

        totalcostD = (corn_series + cassava_series 
                      + beans_series + straw_series 
                      + bypass_fat_series + CP_005_21P_series 
                      + NaHCO3_series + CP_milk2_series
                     )

        df = pd.DataFrame({
            'corn': corn_series,
            'cassava': cassava_series,
            'beans': beans_series,
            'straw': straw_series,
            'bypass_fat': bypass_fat_series,
            'CP_005_21P': CP_005_21P_series,
            'CP_milk2' : CP_milk2_series,
            'NaHCO3': NaHCO3_series,

            'totalcostD': totalcostD
        })                
        df.index = self.rng_daily
        self.totalcost_D_df = df          
           
        return self.totalcost_D_df
            

    def create_total_feedcostByGroup(self):
        feedcost1f  = pd.DataFrame(self.totalcost_F_df  ['totalcostF'])        
        feedcost1a  = pd.DataFrame(self.totalcost_A_df  ['totalcostA'])
        feedcost1b  = pd.DataFrame(self.totalcost_B_df  ['totalcostB'])
        feedcost1c  = pd.DataFrame(self.totalcost_C_df  ['totalcostC'])
        feedcost1d  = pd.DataFrame(self.totalcost_D_df  ['totalcostD'])
        feedcost_daily1   = pd.concat((feedcost1f, feedcost1a, feedcost1b, feedcost1c, feedcost1d), axis=1)
        feedcost_daily1.index.name = 'datex'

        # Monthly aggregation
        feedcost_monthly1 = pd.DataFrame(feedcost_daily1)
        feedcost_monthly1['year']  = feedcost_daily1.index.year
        feedcost_monthly1['month'] = feedcost_daily1.index.month
        feedcost_monthly1['days']  = feedcost_daily1.index.days_in_month    
        self.feedcost_monthly  = feedcost_monthly1.groupby(['year','month', 'days']).agg('sum')

        # Weekly aggregation
        feedcost_weekly1 = feedcost_daily1.copy()
        feedcost_weekly1['week'] = feedcost_weekly1.index.to_series().dt.to_period('W').apply(lambda r: r.start_time)
        self.feedcost_weekly = feedcost_weekly1.groupby('week').agg('sum').reset_index()

        self.feedcost_daily = feedcost_daily1
        return self.feedcost_daily, self.feedcost_monthly, self.feedcost_weekly
    
    def write_to_csv(self):
        
        self.current_feedcost       .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\current_feedcost.csv')
        self.unit_prices_daily      .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\unit_prices_daily.csv')
        self.totalcost_D_df         .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\totalcost_D_df.csv')
        # self.last_values_all_df     .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\last_values_all_df.csv')
                
        self.feedcost_daily     .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup_daily.csv')
        self.feedcost_weekly    .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup_weekly.csv')
        self.feedcost_monthly   .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup_monthly.csv')
        

if __name__ == "__main__":
    obj = Feedcost_basics()
    obj.load_and_process()
    
                 