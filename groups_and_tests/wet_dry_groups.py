'''groups_and_tests.wet_dry_groups'''
import inspect
import pandas as pd
# import numpy as py
from container import get_dependency

#NOTE: the reason for creating a df rather than a dict is that later slicing and manipulation is easier with a csv 
        #don't create a json.....
#NOTE: the reason for using weekly data is that daily is too volatile for grouping        

class WetDryGroups:
    def __init__(self):
        print(f"WetDryGroups instantiated by: {inspect.stack()[1].filename}")

        self.startdate  = None
        self.fcb_weekly  = None        
        self.WDDict     = None
        self.WDDf       = None

        self.GWDDf      = None
        self.FGWDDf     = None
        self.net_revenue_wet_dry_df = None

    def load_and_process(self):
        FCB = get_dependency('feedcost_basics')
        DR = get_dependency('date_range')
        WD = get_dependency('wet_dry')

        self.startdate = DR.startdate
        self.fcb_weekly = FCB.feedcost_weekly

        WDD = WD.wet_dry_weekly.copy()  # NOTE: weekly data - not daily, now uses 'period_week' not 'week'
        # Filter by date if present, otherwise use all
        if 'date' in WDD.columns:
            self.WDDf = WDD.loc[WDD['date'] >= pd.to_datetime(self.startdate)]
        else:
            self.WDDf = WDD.copy()

        self.GWDDf    = self.assign_groups()
        self.FGWDDf   = self.create_feedcost_key()
        self.net_revenue_wet_dry_df     = self.create_wet_dry_df_with_net_revenue()
        self.write_to_csv_json()

    def assign_groups(self):
        grouped_df = self.WDDf.copy()
        # Get pregnant_mask from insem_ultra_data
        IUD = get_dependency('insem_ultra_data')
        preg = IUD.all_preg.reset_index(drop=True)
        pregnant_mask1 = preg[['WY_id', 'status']]
        pregnant_mask2 = pregnant_mask1.loc[pregnant_mask1['status'] == 'M'].reset_index(drop=True)
        pregnant_mask = pd.Series(pregnant_mask2['WY_id'])

        # Sort by WY_id, period, and period_week, then reset index
        sort_cols = ['WY_id', 'period', 'period_week'] if 'period' in grouped_df.columns and 'period_week' in grouped_df.columns else ['WY_id', 'period_week'] if 'period_week' in grouped_df.columns else ['WY_id', 'date']
        grouped_df = grouped_df.sort_values(sort_cols).reset_index(drop=True)

        def assign_group(row):
            # Use period_week as the week counter
            week_num = row['period_week'] if 'period_week'  in row else row.get('week_num', None)
            day_num  = row['day_num']     if 'day_num'      in row else row.get( None) 
            liters = row['liters']
            wy_id = row['WY_id'] if 'WY_id' in row else None
            if pd.isna(week_num) or pd.isna(day_num) or pd.isna(liters):
                return None
            if day_num < 21 and liters >0:
                return 'F'
            elif day_num >= 21 and liters >= 15:
                return 'A'
            elif day_num >= 21 and 0 < liters < 15:
                if wy_id in pregnant_mask.values:
                    return 'C'
                else:
                    return 'B'
            elif liters == 0:
                return 'D'
            else:
                return None

        grouped_df['group'] = grouped_df.apply(assign_group, axis=1)
        self.GWDDf = grouped_df
        return self.GWDDf

    def create_feedcost_key(self):
        feedcost_df = self.GWDDf.copy()
        fcb = self.fcb_weekly.copy()
        # Use only 'week' as the date column
        if fcb.index.name != 'week':
            fcb = fcb.reset_index()
        if 'week' not in fcb.columns:
            raise KeyError("feedcost_weekly must have a 'week' column.")
        fcb['week'] = pd.to_datetime(fcb['week'])
        group_to_col = {'F': 'totalcostF', 'A': 'totalcostA', 'B': 'totalcostB', 'C': 'totalcostC', 'D': 'totalcostD'}

        def get_feedcost(row):
            group = row['group']
            # Use the actual date column for week matching
            week_date = row['date'] if 'date' in row else None
            if pd.isna(group) or pd.isna(week_date):
                return None
            col = group_to_col.get(group)
            if col is None:
                return None
            # Convert date to period start (week) to match fcb['week']
            try:
                week_dt = pd.to_datetime(week_date).to_period('W').start_time
            except Exception:
                return None
            match = fcb[fcb['week'] == week_dt]
            if not match.empty and col in match.columns:
                try:
                    return round(float(match.iloc[0][col]), 1)
                except Exception:
                    return None
            return None
        # Ensure GWDDf has 'week' column
        if 'date' in feedcost_df.columns:
            feedcost_df['week'] = feedcost_df['date'].dt.to_period('W').apply(lambda r: r.start_time)
        feedcost_df['feedcost'] = feedcost_df.apply(get_feedcost, axis=1)
        self.FGWDDf = feedcost_df
        return self.FGWDDf



    def create_wet_dry_df_with_net_revenue(self):
        net_revenue_df = self.FGWDDf.copy()
        # Multiply revenue by 7 to get weekly revenue
        if 'revenue' in net_revenue_df.columns:
            net_revenue_df['revenue'] = net_revenue_df['revenue'] * 7
        def calc_net_revenue(row):
            revenue = row.get('revenue')
            feedcost = row.get('feedcost')
            if pd.isna(revenue) or pd.isna(feedcost):
                return None
            try:
                return round(float(revenue) - float(feedcost), 1)
            except Exception:
                return None
        net_revenue_df['net_revenue'] = net_revenue_df.apply(calc_net_revenue, axis=1)
        self.net_revenue_wet_dry_df = net_revenue_df
        return self.net_revenue_wet_dry_df
    
# NOTE there is no dict created here -- could be, if needed later but downstream only uses DF


    def write_to_csv_json(self):

        self.net_revenue_wet_dry_df.to_csv(r"F:\\COWS\\data\\groups_and_tests\\WDD_flat_with_net_revenue.csv", index=False)
        self.net_revenue_wet_dry_df.to_json(r"F:\\COWS\\data\\groups_and_tests\\WDD_flat_with_net_revenue.json", orient='records', indent=2, date_format='iso')


if __name__ == "__main__":
    obj = WetDryGroups()
    obj.load_and_process()