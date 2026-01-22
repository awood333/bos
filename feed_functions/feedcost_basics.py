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
        self.FS = None
        self.data_loader = None
        self.feed_type = ['corn','cassava','beans','straw', 'bypass_fat',  'NaHCO3', 
                          'CP_005_21P', 'CP_005_DSW', 'CP_970_Plus', 'CP_973GM','CP_milk2', 'CP_power_starch' ]
        self.rng_monthly = None
        self.rng_monthly2 = None
        self.rng_daily = None

        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        self.feed_series_dict = {}
        self.last_values_all_df = None
        # self.current_feedcost = None
        # self.unit_prices_daily = None
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
        self.FS = get_dependency('feedcost_sequences')
        self.data_loader = DataLoader('F:/COWS/data/feed_data/feed_csv')

        self.rng_monthly  = self.DR.date_range_monthly
        self.rng_monthly2 = getattr(self.DR, 'date_range_monthly2', None)
        self.rng_daily    = self.DR.date_range_daily

        self.price_seq_dict, self.daily_amt_dict = self.create_feed_cost_dict()
        self.feed_series_dict = self.create_separate_feed_series()
        self.create_cost_group('F', 'fresh_kg')
        self.create_cost_group('A', 'group_a_kg')
        self.create_cost_group('B', 'group_b_kg')
        self.create_cost_group('C', 'group_c_kg')
        self.create_cost_group('D', 'dry_kg')
        self.create_total_cost_group('F')
        self.create_total_cost_group('A')
        self.create_total_cost_group('B')
        self.create_total_cost_group('C')
        self.create_total_cost_group('D')
        self.last_values_all_df = self.create_last_values()
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

    def create_cost_group(self, group_name, kg_column):
        cost_dict = {feed: [] for feed in self.feed_type}
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                kg = self.daily_amt_dict[feed].loc[i, kg_column]
                cost = unit_price * kg
                cost_dict[feed].append(cost)
        setattr(self, f'cost_dict_{group_name}', cost_dict)
        return cost_dict
    
    
    def create_total_cost_group(self, group_name):
        cost_dict = getattr(self, f'cost_dict_{group_name}')
        feeds_in_group = [feed for feed in self.feed_type if feed in cost_dict]
        feed_series = {feed: pd.Series(cost_dict[feed]).astype(float) for feed in feeds_in_group}
        total_cost = sum(feed_series.values())
        df = pd.DataFrame(feed_series)
        df[f'totalcost{group_name}'] = total_cost
        df.index = self.rng_daily
        setattr(self, f'totalcost_{group_name}_df', df)
        return df 

    
    def create_last_values(self):
        # Create a table with feed types as index, groups as columns, values are last in each group's total cost DataFrame
        group_names = ['F', 'A', 'B', 'C', 'D']
        last_values = {}
        for group in group_names:
            df = getattr(self, f'totalcost_{group}_df', None)
            if df is not None:
                # Use the group total cost column
                col = f'totalcost{group}'
                last_values[group] = df[col].iloc[-1]
            else:
                last_values[group] = pd.Series([float('nan')] * len(self.feed_type), index=self.feed_type)
        # Combine into DataFrame: index=feed_type, columns=groups
        last_values_df = pd.DataFrame(last_values, index=self.feed_type)
        self.last_values_all_df = last_values_df
        return self.last_values_all_df
    
        
    def create_total_feedcostByGroup(self):
        # Ensure these are DataFrames, not methods or other types
        def get_group_df(df, col):
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Expected DataFrame for group, got {type(df)}")
            return df[[col]]

        feedcost1f  = get_group_df(self.totalcost_F_df, 'totalcostF')
        feedcost1a  = get_group_df(self.totalcost_A_df, 'totalcostA')
        feedcost1b  = get_group_df(self.totalcost_B_df, 'totalcostB')
        feedcost1c  = get_group_df(self.totalcost_C_df, 'totalcostC')
        feedcost1d  = get_group_df(self.totalcost_D_df, 'totalcostD')
        feedcost_daily1   = pd.concat((feedcost1f, feedcost1a, feedcost1b, feedcost1c, feedcost1d), axis=1)
        feedcost_daily1.index.name = 'datex'

        # Monthly aggregation
        feedcost_monthly1 = pd.DataFrame(feedcost_daily1)
        feedcost_monthly1['year']  = feedcost_daily1.index.year
        feedcost_monthly1['month'] = feedcost_daily1.index.month
        feedcost_monthly1['days']  = feedcost_daily1.index.days_in_month    
        self.feedcost_monthly  = feedcost_monthly1.groupby(['year','month', 'days']).agg('sum')
        self.feedcost_monthly['sum'] = self.feedcost_monthly.sum(axis=1)

        # Weekly aggregation
        feedcost_weekly1 = feedcost_daily1.copy()
        feedcost_weekly1['week'] = feedcost_weekly1.index.to_series().dt.to_period('W').apply(lambda r: r.start_time)
        self.feedcost_weekly = feedcost_weekly1.groupby('week').agg('sum').reset_index()
        # Only sum numeric columns, exclude 'week' (which is a Timestamp)
        numeric_cols = [col for col in self.feedcost_weekly.columns if col != 'week']
        self.feedcost_weekly['sum'] = self.feedcost_weekly[numeric_cols].sum(axis=1)


        self.feedcost_daily = feedcost_daily1
        return self.feedcost_daily, self.feedcost_monthly, self.feedcost_weekly
    
    def write_to_csv(self):
        
        self.feedcost_daily     .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup_daily.csv')
        self.feedcost_weekly    .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup_weekly.csv')
        self.feedcost_monthly   .to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup_monthly.csv')
        

if __name__ == "__main__":
    obj = Feedcost_basics()
    obj.load_and_process()
    
                 