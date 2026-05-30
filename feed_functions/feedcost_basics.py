'''feed_functions\\feedcost_basics.py'''
import inspect
import os       #don't erase
import pandas as pd
from container import get_dependency
from config_path import GDRIVE_FEED_INVOICE_DATA, GDRIVE_FEED_DAILY_AMT_DATA, LOCAL_FEEDCOST_BY_GROUP, MASTER_FEED_INVOICE_SHEET_ID, MASTER_FEED_DAILY_AMT_SHEET_ID
from utilities.gdrive_loader import gdrive_read_sheet_tab

class DataLoader:
    def __init__(self, base_path, sheet_id=None):  #see load and process data_loader for actual address
        self.base_path = base_path
        self.sheet_id = sheet_id

    def load_csv(self, filename):
        path = os.path.join(self.base_path, filename)
        data = pd.read_csv(path, header=0).dropna(how='all')
        data['datex'] = pd.to_datetime(data['datex'], errors='coerce')
        data = data.set_index(data['datex']).drop(columns=['datex'])
        data = data.sort_index()
        return data

    def load_invoice_csv(self, feed):
        tab_name = f'{feed}_invoice_detail'
        df = gdrive_read_sheet_tab(self.sheet_id, tab_name)
        df = df.reset_index()  # first col ('year') was auto-set as index
        df.columns = df.columns.str.strip()
        df['Invoice date'] = pd.to_datetime(df['Invoice date'], errors='coerce')
        df = df.dropna(subset=['Invoice date'])
        df = df.set_index('Invoice date')[['price/kg']].rename(columns={'price/kg': 'unit_price'})
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        df = df[~df.index.duplicated(keep='last')]
        return df.sort_index()

    def load_daily_amt_sheet(self, feed):
        tab_name = f'{feed}_daily_amt'
        df = gdrive_read_sheet_tab(self.sheet_id, tab_name)
        df = df.reset_index()
        df.columns = df.columns.str.strip()
        df['datex'] = pd.to_datetime(df['datex'], errors='coerce')
        df = df.dropna(subset=['datex'])
        df = df.set_index('datex')
        df = df.apply(pd.to_numeric, errors='coerce')
        df = df[~df.index.duplicated(keep='last')]
        return df.sort_index()
        

class Feedcost_basics:

    def __init__(self):
        print(f"Feedcost_basics instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.data_loader = None
        self.price_loader = None
        self.amt_loader   = None
        self.feed_type = ['corn','cassava','beans','straw',   'NaHCO3', 
                          'CP_005_21P', 'CP_005_DSW', 'CP_970_Plus', 'CP_973GM','CP_milk2', 'CP_power_starch' ]
        self.rng_monthly = None
        self.rng_monthly2 = None
        self.rng_daily = None

        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        self.feed_series_dict = {}
        self.feed_series_full_df = None
        self.feed_series_last_row_T = None
        self.last_values_all_df = None
        # self.current_feedcost = None
        # self.unit_prices_daily = None
        self.daily_cost_dict_F = {}
        self.last_cost_details_F_df = None        
        self.daily_cost_dict_A = {}
        self.last_cost_details_A_df = None
        self.daily_cost_dict_B = {}
        self.last_cost_details_B_df = None
        self.daily_cost_dict_C = {}
        self.last_cost_details_C_df = None
        self.daily_cost_dict_D = {}
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
        
        self.price_loader = DataLoader(GDRIVE_FEED_INVOICE_DATA, sheet_id=MASTER_FEED_INVOICE_SHEET_ID)
        self.amt_loader   = DataLoader(GDRIVE_FEED_DAILY_AMT_DATA, sheet_id=MASTER_FEED_DAILY_AMT_SHEET_ID)

        self.rng_monthly  = self.DR.date_range_monthly
        self.rng_monthly2 = getattr(self.DR, 'date_range_monthly2', None)
        self.rng_daily    = self.DR.date_range_daily

        self.price_seq_dict, self.daily_amt_dict        = self.create_feed_daily_cost_dict()
        self.feed_series_dict, self.feed_series_full_df = self.create_separate_feed_series()
        # Register the last row (transposed) of feed_series_full_df
        if self.feed_series_full_df is not None and not self.feed_series_full_df.empty:
            self.feed_series_last_row_T = self.feed_series_full_df.iloc[[-1]]
        else:
            self.feed_series_last_row_T = None

        self.daily_cost_dict_F    = self.calc_daily_feed_costs ('fresh_kg')
        self.daily_cost_dict_A    = self.calc_daily_feed_costs ('group_a_kg')
        self.daily_cost_dict_B    = self.calc_daily_feed_costs ('group_b_kg')
        self.daily_cost_dict_C    = self.calc_daily_feed_costs ('group_c_kg')
        self.daily_cost_dict_D    = self.calc_daily_feed_costs ('dry_kg')
        self.totalcost_F_df = self.create_total_cost_group(self.daily_cost_dict_F, 'F')
        self.totalcost_A_df = self.create_total_cost_group(self.daily_cost_dict_A, 'A')
        self.totalcost_B_df = self.create_total_cost_group(self.daily_cost_dict_B, 'B')
        self.totalcost_C_df = self.create_total_cost_group(self.daily_cost_dict_C, 'C')
        self.totalcost_D_df = self.create_total_cost_group(self.daily_cost_dict_D, 'D')
        self.last_values_all_df = self.create_last_values()
        self.feedcost_daily, self.feedcost_monthly, self.feedcost_weekly = self.create_total_feedcostByGroup()
        self.write_to_csv()

    def create_feed_daily_cost_dict(self):
        self.price_seq_dict = {}
        self.daily_amt_dict = {}
        
        for feed in self.feed_type:
            price_seq = self.price_loader.load_invoice_csv(feed)
            daily_amt = self.amt_loader.load_daily_amt_sheet(feed)
            
            price_seq = price_seq.reindex(self.rng_daily, method='ffill')
            daily_amt = daily_amt.reindex(self.rng_daily, method='ffill')
            
            self.price_seq_dict[feed] = price_seq
            self.daily_amt_dict[feed] = daily_amt
            
        return self.price_seq_dict, self.daily_amt_dict
    
    def create_separate_feed_series(self):
        psd = self.price_seq_dict
        dad = self.daily_amt_dict

        # Create the feed_series_dict as before
        self.feed_series_dict = {
            feed: {
                'psd': psd[feed],
                'dad': dad[feed]
            } for feed in self.feed_type
        }

        # Convert feed_series_dict to a DataFrame
        # Each feed will have two columns: psd and dad, each is a DataFrame indexed by date
        # We'll concatenate all 'psd' and all 'dad' into MultiIndex columns
        psd_df = pd.concat({feed: self.feed_series_dict[feed]['psd'] for feed in self.feed_type}, axis=1)
        dad_df = pd.concat({feed: self.feed_series_dict[feed]['dad'] for feed in self.feed_type}, axis=1)

        # Combine psd and dad into a single DataFrame with MultiIndex columns
        self.feed_series_full_df = pd.concat({'psd': psd_df, 'dad': dad_df}, axis=1)

        return self.feed_series_dict, self.feed_series_full_df

    def calc_daily_feed_costs (self, kg_column):
        daily_cost_dict = {feed: [] for feed in self.feed_type}
        for i in self.rng_daily:
            for feed in self.feed_type:
                unit_price = self.price_seq_dict[feed].loc[i, 'unit_price']
                kg = self.daily_amt_dict[feed].loc[i, kg_column]
                cost = unit_price * kg
                daily_cost_dict[feed].append(cost)

        # returns a dict with the daily cost of each feed type -- price seq * daily amt 
        return daily_cost_dict
    
    
    def create_total_cost_group(self, daily_cost_dict, group_name):
        feeds_in_group = [feed for feed in self.feed_type if feed in daily_cost_dict]
        feed_series = {feed: pd.Series(daily_cost_dict[feed]).astype(float) for feed in feeds_in_group}
        df = pd.DataFrame(feed_series)
        df = df.fillna(0)
        total_cost = df.sum(axis=1)
        df[f'totalcost{group_name}'] = total_cost
        df.index = self.rng_daily
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
        LOCAL_FEEDCOST_BY_GROUP.mkdir(parents=True, exist_ok=True)
        self.feedcost_daily     .to_csv(LOCAL_FEEDCOST_BY_GROUP / "feedcostByGroup_daily.csv")
        self.feedcost_weekly    .to_csv(LOCAL_FEEDCOST_BY_GROUP / "feedcostByGroup_weekly.csv")
        self.feedcost_monthly   .to_csv(LOCAL_FEEDCOST_BY_GROUP / "feedcostByGroup_monthly.csv")
        if self.feed_series_last_row_T is not None:
            self.feed_series_last_row_T.to_csv(LOCAL_FEEDCOST_BY_GROUP / "feedcostByGroup_last.csv")
        

if __name__ == "__main__":
    obj = Feedcost_basics()
    obj.load_and_process()
    
                 