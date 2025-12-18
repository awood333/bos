'''milk_functions.lactation.net_rev_this_lactation_WB.py'''

import inspect
import pandas as pd
import json
import math
from container import get_dependency

class NetRevThisLactation_WB():
    def __init__(self):
        ''' based on the whiteboard, creates a df with net_revenue/cow/day from any given date for all live cows
         also creates the dict with  '''

        print(f"NetRevThisLactation_WB instantiated by: {inspect.stack()[1].filename}")

        self.start_date = None
        self.MB = None
        self.WD = None
        self.SD2= None
        self.alive_mask = None
        self.milk = None
        self.milk_income = None
        self.LB = None
        self.WBG = None
        self.FB = None

        self.feed = None
        self.group = None

        self.milking_wkly = None
        self.milking_daily = None

        self.max_calfnum_bdate_df=None
        self.nested_dict_by_date = {}
        self.nested_dict_by_group = {}
        self.net_revenue_WB = None

    
    def load_and_process(self):

        self.start_date = '2025-09-01'
        self.MB = get_dependency('milk_basics')
        self.WD = get_dependency('wet_dry')
        self.SD2= get_dependency('status_data2')
        self.alive_mask = self.SD2.alive_mask['WY_id'].astype(str).reset_index(drop=True)        
        self.LB = pd.read_csv("F:\\COWS\\data\\csv_files\\live_births.csv", index_col=None)
        self.WBG = get_dependency('whiteboard_groups')
        self.FB = get_dependency('feedcost_basics')
        
        #set up feed
        feed1 = self.FB.feedcost_daily.loc[self.start_date:,:]
        feed1 = feed1.rename(columns={
            'totalcostF': 'F',
            'totalcostA': 'A',
            'totalcostB': 'B',
            'totalcostC': 'C',
            'totalcostD': 'D'
        })

        self.feed = feed1.round(0)
        #get and slice milk by startdate
        milk1 = pd.read_csv("F:\\COWS\\data\\milk_data\\fullday\\fullday.csv", index_col=0)
        milk2 = milk1.T
        self.alive_mask = self.SD2.alive_mask['WY_id'].astype(str).reset_index(drop=True)
        milk3 = milk2.loc[self.alive_mask].T
        self.milk = milk3.loc[self.start_date:,:]
        self.milk_income = (self.milk * 22).round(0)    
        # Get group info
        self.group = self.WBG.groups_by_date_by_cow.loc[self.start_date:, self.milk.columns]

        # part of the loadAndProcess --  Calculate feed cost per cow per day
        def get_feedcost(row):
            date = row.name
            return row.apply(lambda grp: self.feed.loc[date, grp] if pd.notna(grp) and grp in self.feed.columns else pd.NA)

        group_feedcost_df = self.group.apply(get_feedcost, axis=1)
        group_feedcost_df.index = pd.to_datetime(group_feedcost_df.index)
        self.milk_income.index = pd.to_datetime(self.milk_income.index)

        self.net_revenue_WB = (self.milk_income - group_feedcost_df).round(0)        


        #methods
        self.max_calfnum_bdate_df   = self.create_max_calfnum_bdate()
        self.nested_dict_by_date = self.build_nested_dict_by_date(
            self.milk,
            self.milk_income,
            self.group,
            group_feedcost_df,
            self.net_revenue_WB
        )
        self.nested_dict_by_group   = self.create_nested_dict_by_group()
        self.create_write_to_csv_json()


    def create_max_calfnum_bdate(self):
        max_calf_num    = self.LB.groupby('WY_id')['calf#']  .max().reset_index()
        max_calf_bdate  = self.LB.groupby('WY_id')['b_date']   .max().reset_index()
        max_calfnum_bdate_df1 = pd.merge(max_calf_num, max_calf_bdate, on='WY_id')

        max_calfnum_bdate_df1['WY_id'] = max_calfnum_bdate_df1['WY_id'].astype(int).astype(str)
        max_calfnum_bdate_df1 = max_calfnum_bdate_df1.set_index('WY_id')
        
        #get and slice LBirths by self.alive_mask
        max_calfnum_bdate_df = max_calfnum_bdate_df1.loc[self.alive_mask]
        max_calfnum_bdate_df = max_calfnum_bdate_df.dropna(subset=['calf#'])
        max_calfnum_bdate_df['calf#'] = max_calfnum_bdate_df['calf#'].astype(int).astype(str)

        self.max_calfnum_bdate_df = max_calfnum_bdate_df
        return self.max_calfnum_bdate_df
    

    def build_nested_dict_by_date(self, milk, milk_income, group, group_feedcost_df, net_revenue_df):
        """
        Build nested dict by date: {date: {wy_id: {...}}}
        """
        nested_dict_by_date = {}
        for date in milk.index:
            nested_dict_by_date[date] = {}
            for wy_id in milk.columns:
                milk_val        = milk.at[date, wy_id] if wy_id in milk.columns else pd.NA
                milk_income_val = milk_income.at[date, wy_id] if wy_id in milk_income.columns else pd.NA
                group_val       = group.at[date, wy_id] if (date in group.index and wy_id in group.columns) else pd.NA
                feedcost_val    = group_feedcost_df.at[date, wy_id] if (date in group_feedcost_df.index and wy_id in group_feedcost_df.columns) else pd.NA
                net_revenue_val = net_revenue_df.at[date, wy_id] if (date in net_revenue_df.index and wy_id in net_revenue_df.columns) else pd.NA

                nested_dict_by_date[date][wy_id] = {
                    'milk': milk_val,
                    'milk_income': milk_income_val,
                    'group': group_val,
                    'feedcost': feedcost_val,
                    'net_revenue': net_revenue_val
                }
        self.nested_dict_by_date = nested_dict_by_date
        return self.nested_dict_by_date


    def create_nested_dict_by_group(self):
        """
        Rearranges self.nested_dict_by_date into a nested dict by group, then date, then cow.
        Structure: {group: {date: {wy_id: {...}}}}
        """
        nested_dict_by_group = {}
        for date, cows in self.nested_dict_by_date.items():
            for wy_id, data in cows.items():
                group = data.get('group', 'Unknown')
                if pd.isna(group):
                    group = 'Unknown'
                if group not in nested_dict_by_group:
                    nested_dict_by_group[group] = {}
                if date not in nested_dict_by_group[group]:
                    nested_dict_by_group[group][date] = {}
                nested_dict_by_group[group][date][wy_id] = data
        self.nested_dict_by_group = nested_dict_by_group
        return self.nested_dict_by_group



    def replace_nan_in_dict(self, obj):
        if isinstance(obj, dict):
            return {str(k): self.replace_nan_in_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_nan_in_dict(v) for v in obj]
        elif obj is None or (isinstance(obj, float) and math.isnan(obj)) or obj is pd.NA:
            return "NaN"
        else:
            return obj 

    
    
    def create_write_to_csv_json(self):
        self.net_revenue_WB.to_csv("F:\\COWS\\data\\milk_data\\groups\\net_revenue_WB.csv")
        # Replace NaN/NA in nested_dict_by_date before saving as JSON
        cleaned_dict_by_date  = self.replace_nan_in_dict(self.nested_dict_by_date)
        cleaned_dict_by_group = self.replace_nan_in_dict(self.nested_dict_by_group)

        with open("F:\\COWS\\data\\milk_data\\groups\\nested_dict_whiteboard_by_date.json", 'w', encoding='utf-8') as f:
            json.dump(cleaned_dict_by_date, f, indent=2, default=str)      
        with open("F:\\COWS\\data\\milk_data\\groups\\nested_dict_whiteboard_by_group.json", 'w', encoding='utf-8') as f:
            json.dump(cleaned_dict_by_group, f, indent=2, default=str)      




if __name__ == "__main__":
    tlbd = NetRevThisLactation_WB()
    tlbd.load_and_process()        