'''milk_functions.lactation.this_lactation_by_date.py'''

import inspect
import pandas as pd
from container import get_dependency

class ThisLactationByDate():
    def __init__(self):
        print(f"ThisLactation instantiated by: {inspect.stack()[1].filename}")

        self.MB = None
        self.WD = None
        self.SD2= None
        self.alive_mask = None
        self.milk = None
        self.LB = None
        self.WBG = None
        self.FB = None

        self.milking_wkly = None
        self.milking_daily = None

        self.max_calfnum_bdate_df=None
        self.nested_dict = {}
        self.net_revenue = None

    
    def load_and_process(self):

        self.start_date = '2025-09-01'
        self.MB = get_dependency('milk_basics')
        self.WD = get_dependency('wet_dry')
        self.SD2= get_dependency('status_data2')
        self.alive_mask = self.SD2.alive_mask['WY_id'].astype(str).reset_index(drop=True)        
        self.LB = pd.read_csv("F:\\COWS\\data\\csv_files\\live_births.csv", index_col=None)
        self.WBG = get_dependency('whiteboard_groups')
        self.FB = get_dependency('feedcost_basics')
        

        #get and slice milk by startdate
        milk1 = pd.read_csv("F:\\COWS\\data\\milk_data\\fullday\\fullday.csv", index_col=0)
        milk2 = milk1.T
        self.alive_mask = self.SD2.alive_mask['WY_id'].astype(str).reset_index(drop=True)
        milk3 = milk2.loc[self.alive_mask].T
        self.milk = milk3.loc[self.start_date:,:]

        self.max_calfnum_bdate_df   = self.create_max_calfnum_bdate()
        self.nested_dict, self.net_revenue            = self.create_this_lact_by_date()
        self.create_write_to_csv()
        
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
        group = self.WBG.groups_by_date_by_cow
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


        print(
            f"milk.shape: {milk.shape}\n"
            f"milk_income.shape: {milk_income.shape}\n"
            f"group.shape: {group.shape}\n"
            f"group_feedcost_df.shape: {group_feedcost_df.shape}\n"
            f"net_revenue_df.shape: {net_revenue_df.shape}\n"
            f"feed.shape: {feed.shape}"
        )


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
        self.net_revenue_df = net_revenue_df

        return self.nested_dict, self.net_revenue        

    
    
    def create_write_to_csv(self):
        self.net_revenue.to_csv("F:\\COWS\\data\\milk_data\\groups\\net_revenue.csv")
    
if __name__ == "__main__":
    tlbd = ThisLactationByDate()
    tlbd.load_and_process()        