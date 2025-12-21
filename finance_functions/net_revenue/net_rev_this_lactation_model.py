'''finance_functions.net_revenue.net_rev_this_lactation_by_date_model.py'''

import inspect
import pandas as pd
import json
import math

from container import get_dependency
from finance_functions.net_revenue.net_revenue_weekly_agg_model import aggregate_net_revenue_weekly

class NetRevThisLactation_model():
    def __init__(self):
        ''' based on the model, creates a df with net_revenue/cow/day from any given date for all live cows
         also creates the dict with  '''
        
        print(f"NetRevThisLactation_model instantiated by: {inspect.stack()[1].filename}")


        self.start_date = None
        self.MB = None
        self.WD = None
        self.SD= None
        self.alive_mask = None
        self.milk = None
        self.LB = None
        self.MG = None
        self.FB = None

        self.milking_wkly = None
        self.milking_daily = None

        self.max_calfnum_bdate_df=None
        self.nested_dict = {}
        self.net_revenue_model = None
        self.net_revenue_model_weekly = None

    
    def load_and_process(self):

        self.start_date = '2025-09-01'
        self.MB = get_dependency('milk_basics')
        self.WD = get_dependency('wet_dry')
        self.SD= get_dependency('statusData')
        self.alive_mask = self.SD.alive_mask['WY_id'].astype(str).reset_index(drop=True)        
        self.LB = pd.read_csv("F:\\COWS\\data\\csv_files\\live_births.csv", index_col=None)
        self.MG = get_dependency('model_groups')
        self.FB = get_dependency('feedcost_basics')
        

        #get and slice milk by startdate
        milk1 = pd.read_csv("F:\\COWS\\data\\milk_data\\fullday\\fullday.csv", index_col=0)
        milk2 = milk1.T
        milk3 = milk2.loc[self.alive_mask].T
        self.milk = milk3.loc[self.start_date:,:]

        self.max_calfnum_bdate_df   = self.create_max_calfnum_bdate()
        self.nested_dict, self.net_revenue_model = self.create_this_lact_by_date()
        # Aggregate weekly net revenue
        self.net_revenue_model_weekly = aggregate_net_revenue_weekly(self.net_revenue_model)
        # Write both daily and weekly net revenue to CSV
        self.net_revenue_model.to_csv("F:\\COWS\\data\\milk_data\\groups\\net_revenue_model.csv")
        self.net_revenue_model_weekly.to_csv("F:\\COWS\\data\\PL_data\\net_revenue_weekly.csv")
        self.create_write_to_csv()
        # Optionally, add saving weekly data to CSV in create_write_to_csv
        
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
    
    def create_this_lact_by_date(self):
        milk = self.milk
        milk_income = (milk * 22)
        group = self.MG.groups_by_date_by_cow.loc[self.start_date:,:]
        feed = self.FB.feedcost_daily.loc[self.start_date:,:]


        feed = feed.rename(columns={
            'totalcostA': 'A',
            'totalcostB': 'B',
            'totalcostC': 'C',
            'totalcostD': 'D'
        })

            # Function to get feed cost for each cow on each date
        def get_feedcost(row):
            date = row.name
            return row.apply(lambda grp: feed.loc[date, grp] if pd.notna(grp) and grp in feed.columns else pd.NA)

        # Apply function across group DataFrame
        group_feedcost_df = group.apply(get_feedcost, axis=1)

        net_revenue_df = milk_income - group_feedcost_df



        # Build nested dict
        nested_dict = {}
        for date in milk.index:
            nested_dict[date] = {}
            for wy_id in milk.columns:
                milk_val = milk.at[date, wy_id] if wy_id in milk.columns else pd.NA
                milk_income_val = milk_income.at[date, wy_id] if wy_id in milk_income.columns else pd.NA
                group_val = group.at[date, wy_id] if (date in group.index and wy_id in group.columns) else pd.NA
                feedcost_val = group_feedcost_df.at[date, wy_id] if (date in group_feedcost_df.index and wy_id in group_feedcost_df.columns) else pd.NA
                net_revenue_val = net_revenue_df.at[date, wy_id] if (date in net_revenue_df.index and wy_id in net_revenue_df.columns) else pd.NA

                nested_dict[date][wy_id] = {
                    'milk': milk_val,
                    'milk_income': milk_income_val,
                    'group': group_val,
                    'feedcost': feedcost_val,
                    'net_revenue': net_revenue_val
                }
            
        self.nested_dict = nested_dict
        self.net_revenue_model = net_revenue_df

        return self.nested_dict, self.net_revenue_model       

    def replace_nan_in_dict(self, obj):
        if isinstance(obj, dict):
            return {str(k): self.replace_nan_in_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_nan_in_dict(v) for v in obj]
        elif obj is None or (isinstance(obj, float) and math.isnan(obj)) or obj is pd.NA:
            return "NaN"
        else:
            return obj 

    
    
    def create_write_to_csv(self):
            # -------------------------------------------------------------
            # Write daily net revenue DataFrame to CSV
            # -------------------------------------------------------------
            if self.net_revenue_model is not None:
                self.net_revenue_model.to_csv("F:\\COWS\\data\\milk_data\\groups\\net_revenue_model.csv")

            # -------------------------------------------------------------
            # Write weekly net revenue DataFrame to CSV
            # -------------------------------------------------------------
            if self.net_revenue_model_weekly is not None:
                self.net_revenue_model_weekly.to_csv("F:\\COWS\\data\\PL_data\\net_revenue_weekly.csv")

            # -------------------------------------------------------------
            # Replace NaN/NA in nested_dict before saving as JSON
            # -------------------------------------------------------------
            cleaned_dict = self.replace_nan_in_dict(self.nested_dict)
            with open("F:\\COWS\\data\\milk_data\\groups\\nested_dict_model.json", 'w', encoding='utf-8') as f:
                json.dump(cleaned_dict, f, indent=2, default=str)

            with open("F:\\COWS\\data\\milk_data\\groups\\nested_dict_WB.json", 'w', encoding='utf-8') as f:
                json.dump(cleaned_dict, f, indent=2, default=str)
            # -------------------------------------------------------------
    
if __name__ == "__main__":
    tlbd = NetRevThisLactation_model()
    tlbd.load_and_process()        