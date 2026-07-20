"""feed_functions\\feedcost_by_group_by_day.py"""

import inspect
import pandas as pd
from container import get_dependency

class FeedCostByGroupByDay:
    def __init__(self):
        print(f"FeedCostByGroupByDay instantiated by: {inspect.stack()[1].filename}")
        
        self.BD = None
        self.DR = None
        self.MG = None
        self.FB = None

        
        #methods
        self.wyids = None
        self.dates = None
        self.groups = None
        self.cost_f = None
        self.cost_a = None
        self.cost_b = None
        self.cost_c = None
        self.cost_d = None
        self.cost_by_group_by_day_df = None
        
    def load(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.MG = get_dependency('model_groups')
        self.FB = get_dependency('feedcost_basics')
        self.process()
        
    def process(self):
        self.wyids  = self.MB.data['bd'].index
        self.dates  = self.DR.date_range_weekly
        self.groups = self.MG.group_df
        self.cost_f = self.FB.feedcost_F_df
        self.cost_a = self.FB.feedcost_A_df
        self.cost_b = self.FB.feedcost_B_df
        self.cost_c = self.FB.feedcost_C_df
        self.cost_d = self.FB.feedcost_D_df
        # self.cost_h = self.FB.feedcost_H_df

     #methods
        self.cost_by_group_by_day_df = self.create_feedcost_by_group()
            
            
    def create_feedcost_by_group(self):
        # dates = pd.to_datetime(self.dates).normalize()
        groups = self.groups.copy()


        cost_map = {
        'F': self.cost_f,
        'A': self.cost_a,
        'B': self.cost_b,
        'C': self.cost_c,
        'D': self.cost_d,
        }


        # df.stack ---- Returns a reshaped DataFrame or Series having a multi-level index 
        # with one or more new inner-most levels compared to the current DataFrame.
         
        # The new inner-most levels are created by pivoting the columns of the current dataframe:
        # if the columns have a single level, the output is a Series;
        # if the columns have multiple levels, the new index level(s) is (are) taken 
        # from the prescribed level(s) and the output is a DataFrame.
        
        #.stack() flattens that entire grid into one long column in a single C-level operation

        # long format: one row per (wy_id, date) -> group letter
        long = groups.stack(future_stack=True).rename('group').reset_index()
        long.columns = ['date', 'wy_id',  'group']

        #squeeze each cost frame down to a Series before concat
        cost_series = {}
        for group, df in cost_map.items():
            series = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df   # totalcostF -> plain Series
            series.index = pd.to_datetime(series.index).normalize()
            cost_series[group] = series


        # build a single date x feed-type cost table, then long-ify it
        cost_wide = pd.concat(cost_series, axis=1)          # columns: F, A, B, C, D (single level)
        cost_long = cost_wide.stack(future_stack=True).rename('cost').reset_index()
        cost_long.columns = ['date', 'group', 'cost']
        
        cost_wide.index = pd.to_datetime(cost_wide.index).normalize()
        cost_long = cost_wide.stack(future_stack=True).rename('cost').reset_index()
        cost_long.columns = ['date', 'group', 'cost']

        merged = long.merge(cost_long, on=['date', 'group'], how='left')

        cost_by_group_df = merged.pivot(index='date', columns='wy_id', values='cost')
        self.cost_by_group_by_day_df = cost_by_group_df
        return self.cost_by_group_by_day_df
            
if __name__ == "__main__":
    obj = FeedCostByGroupByDay()
    obj.load()