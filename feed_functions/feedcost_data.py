'''feedcost_data.py'''

import inspect
import pandas as pd
import numpy as np
from container import get_dependency
from config_path import LOCAL_FEED_CONSUMPTION

class FeedcostData:
    def __init__(self):
        print(f"FeedcostData instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.FCB = None
        self.MG = None
        self.SD = None
        self.dateRange = None
        self.herd_daily = None
        self.groups_count_daily = None
        self.feed_types = None
        self.results = {}

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.FCB= get_dependency('feedcost_basics')
        self.MG = get_dependency('model_groups')
        self.SD = get_dependency('status_data')

        self.dateRange = self.DR.date_range_daily
        self.herd_daily= self.SD.herd_daily
        # Reindex groups_count_daily from weekly to daily using ffill

        groups_count = self.MG.groups_count_daily.copy()
        daily_index = pd.date_range(groups_count.index.min(), groups_count.index.max(), freq='D')
        group_cols = ['F', 'A', 'B', 'C', 'D']
        # Create a DataFrame with NaN everywhere
        daily_groups = pd.DataFrame(np.nan, index=daily_index, columns=groups_count.columns)
        # Only set group columns for weekly dates, leave all other days as NaN
        for idx in groups_count.index:
            if idx in daily_groups.index:
                for col in group_cols:
                    val = groups_count.at[idx, col]
                    # Only set if the value is not zero (treat zeros as NaN for ffill)
                    if val != 0:
                        daily_groups.at[idx, col] = val
        # Forward fill only the group columns
        for col in group_cols:
            if col in daily_groups.columns:
                daily_groups[col] = daily_groups[col].ffill()
        # Fill any remaining NaN with 0
        daily_groups[group_cols] = daily_groups[group_cols].fillna(0)
        # Recompute totals if present
        if 'totals' in daily_groups.columns:
            daily_groups['totals'] = daily_groups[group_cols].sum(axis=1)
        self.groups_count_daily = daily_groups
        self.feed_types = self.FCB.feed_type

        for feed in self.feed_types:
            try:
                # Get price sequence from FCB (already loaded from invoice data)
                price_seq = self.FCB.price_seq_dict[feed]

                # Get daily amount
                daily_amt = self.FCB.feed_series_dict[feed]['dad']
                groups_count = self.groups_count_daily
                herd_daily_dry = self.herd_daily[['dry']]
                daily_amt = pd.concat([daily_amt, groups_count, herd_daily_dry], axis=1).fillna(0)

                # Calculate group amounts
                daily_amt['fresh_amt']   = daily_amt.get('fresh_kg', 0)   * daily_amt.get('F', 0)
                daily_amt['group_a_amt'] = daily_amt.get('group_a_kg', 0) * daily_amt.get('A', 0)
                daily_amt['group_b_amt'] = daily_amt.get('group_b_kg', 0) * daily_amt.get('B', 0)
                daily_amt['group_c_amt'] = daily_amt.get('group_c_kg', 0) * daily_amt.get('C', 0)
                daily_amt['dry_amt']     = daily_amt.get('dry_kg', 0)     * daily_amt.get('D', 0)

                daily_amt['total_amt'] = daily_amt[['fresh_amt','group_a_amt','group_b_amt','dry_amt']].sum(axis=1)

                # Cost sequence
                dcs = pd.concat((daily_amt, price_seq), axis=1)
                dcs['daily cost'] = dcs['unit_price'] * dcs['total_amt']


                # write cost seq and daily amt dfs
                LOCAL_FEED_CONSUMPTION.mkdir(parents=True, exist_ok=True)
                dcs.to_csv(LOCAL_FEED_CONSUMPTION / f"cost_sequence_{feed}.csv")
                daily_amt.to_csv(LOCAL_FEED_CONSUMPTION / f"daily_amt_{feed}.csv")

                self.results[feed] = {
                    'cost_sequence': dcs,
                    'daily_amt': daily_amt
                }
            except Exception as e:
                print(f"Error processing {feed}: {e}")

if __name__ == "__main__":
    obj = FeedcostData()
    obj.load_and_process()
