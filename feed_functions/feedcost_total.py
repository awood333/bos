'''feed_functions.feedcost_total.py'''

import inspect
import pandas as pd
from container import get_dependency


class Feedcost_total:
    def __init__(self):
        print(f"Feedcost_total instantiated by: {inspect.stack()[1].filename}")
        self.FCB = None
        self.feedcost = None
        self.total_feedcost_details_last = None
        self.total_feedcost_monthly = None
        self.total_feedcost_weekly = None
        self.feed_cost_objs = {}
        self.feed_type = None
        self.feedcost_data = None


    def load_and_process(self):
        # Get feed_type from Feedcost_basics
        self.FCB = get_dependency('feedcost_basics')
        self.feed_type = self.FCB.feed_type
        # Get all feed cost data from feedcost_data
        self.feedcost_data = get_dependency('feedcost_data')
        self.feedcost, self.total_feedcost_details_last = self.aggregate_feedcosts_from_data()
        self.total_feedcost_monthly = self.create_monthly()
        self.total_feedcost_weekly = self.create_weekly()
        self.write_to_csv()

    # No longer needed: load_feedcost_objects

    def aggregate_feedcosts_from_data(self):
        # Use feedcost_data.results to aggregate all feeds
        cost_frames = []
        last_rows = []
        print("Aggregating feedcosts from feedcost_data for feeds:", self.feed_type)
        for feed in self.feed_type:
            feed_result = self.feedcost_data.results.get(feed)
            if feed_result and 'cost_sequence' in feed_result:
                df = feed_result['cost_sequence']
                if isinstance(df, pd.DataFrame) and 'daily cost' in df.columns:
                    col_name = f'{feed} daily cost'
                    feed_df = df.loc[:, ['daily cost']].rename(columns={'daily cost': col_name})
                    print(f"  - {feed}: shape {feed_df.shape}, last index {feed_df.index[-1] if not feed_df.empty else 'EMPTY'}")
                    cost_frames.append(feed_df)
                    if not feed_df.empty:
                        last_rows.append(feed_df.iloc[[-1], :])
                else:
                    print(f"  - {feed}: cost_sequence missing or not a DataFrame with 'daily cost'")
            else:
                print(f"  - {feed}: not found in feedcost_data.results")
        if not cost_frames:
            raise ValueError("No feed cost data found in feedcost_data.results.")
        tfc = pd.concat(cost_frames, axis=1)
        tfc['total feedcost'] = tfc.sum(axis=1)
        if last_rows:
            tfc_details = pd.concat(last_rows, axis=0)
        else:
            tfc_details = pd.DataFrame()
        print(f"Final tfc_details shape: {tfc_details.shape}")
        return tfc, tfc_details

    # (Removed duplicate create_feedcost_total method)

    def create_monthly(self):
        fct = self.feedcost['total feedcost'].to_frame()
        fct.index = pd.to_datetime(fct.index)
        fct['year'] = fct.index.year
        fct['month'] = fct.index.month
        fctm1 = fct.groupby(['year','month']).agg({'total feedcost' : 'sum'})
        self.total_feedcost_monthly = fctm1
        return self.total_feedcost_monthly

    def create_weekly(self):
        fct = self.feedcost['total feedcost'].to_frame()
        fct.index = pd.to_datetime(fct.index)
        fct['week'] = fct.index.to_series().dt.to_period('W').apply(lambda r: r.start_time)
        fctw1 = fct.groupby('week').agg({'total feedcost': 'sum'}).reset_index()
        self.total_feedcost_weekly = fctw1
        return self.total_feedcost_weekly

    def write_to_csv(self):
        self.feedcost                   .to_csv(r'F:\COWS\data\feed_data\feedcost_by_group\feedcost.csv')
        self.total_feedcost_details_last.to_csv(r'F:\COWS\data\feed_data\feedcost_by_group\total_feedcost_details_last.csv')
        self.total_feedcost_monthly     .to_csv(r'F:\COWS\data\feed_data\feedcost_by_group\total_feedcost_monthly.csv')
        self.total_feedcost_weekly      .to_csv(r'F:\COWS\data\feed_data\feedcost_by_group\total_feedcost_weekly.csv')

if __name__ == "__main__":
    obj = Feedcost_total()
    obj.load_and_process()