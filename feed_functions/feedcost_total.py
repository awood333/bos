'''feed_functions.feedcost_total.py'''

import inspect
import pandas as pd
from container import get_dependency


class Feedcost_total:



    def __init__(self):
        print(f"Feedcost_total instantiated by: {inspect.stack()[1].filename}")
        self.FCB = None
        self.feed_type = None
        self.feedcost_data = None
        # Dict-based method outputs
        self.feed_cost_from_dict = None
        self.feed_cost_from_dict_details = None
        # DataFrame-based method outputs
        self.feedcost_from_df = None
        self.feedcost_from_df_details = None
        # Common outputs (used by downstream methods)
        self.feedcost = None
        self.total_feedcost_details_last = None
        self.total_feedcost_monthly = None
        self.total_feedcost_weekly = None
        self.feed_cost_objs = {}
        self.total_feedcost_monthly_from_df = None
        self.fct_monthly_from_df = None


    def load_and_process(self, use_df_method=True):
        # Get feed_type from Feedcost_basics
        self.FCB = get_dependency('feedcost_basics')
        self.feed_type = self.FCB.feed_type
        # Get all feed cost data from feedcost_data
        self.feedcost_data = get_dependency('feedcost_data')

        # Dict-based method
        self.feed_cost_from_dict, self.feed_cost_from_dict_details = self.aggregate_feedcosts_from_dict()

        # DataFrame-based method: build amt_df and price_df from results
        amt_df, price_df = self.build_amt_and_price_dfs()
        if not amt_df.empty and not price_df.empty:
            self.feedcost_from_df, self.feedcost_from_df_details = self.aggregate_feedcosts_from_df(amt_df, price_df)
        else:
            self.feedcost_from_df, self.feedcost_from_df_details = None, None

        # Set the main outputs for downstream methods (choose preferred method)
        if use_df_method and self.feedcost_from_df is not None:
            self.feedcost = self.feedcost_from_df
            self.total_feedcost_details_last = self.feedcost_from_df_details
        else:
            self.feedcost = self.feed_cost_from_dict
            self.total_feedcost_details_last = self.feed_cost_from_dict_details

        self.total_feedcost_monthly = self.create_monthly_from_dict()
        self.total_feedcost_weekly = self.create_weekly_from_dict()
        self.total_feedcost_monthly_from_df = self.create_total_feedcost_monthly_from_df()
        self.write_to_csv()

    def build_amt_and_price_dfs(self):
        """
        Build amt_df and price_df from self.feedcost_data.results.
        amt_df: columns [datex, feed_type, fresh_kg, group_a_kg, ...]
        price_df: columns [datex, feed_type, weight, unit_price, ...]
        """
        amt_rows = []
        price_rows = []
        for feed in self.feed_type:
            feed_result = self.feedcost_data.results.get(feed)
            if not feed_result:
                continue
            # daily_amt for amt_df (date is the index, reset to make 'datex' a column)
            if 'daily_amt' in feed_result:
                daily_amt_df = feed_result['daily_amt']
                if isinstance(daily_amt_df, pd.DataFrame):
                    df = daily_amt_df.copy()
                    df.index.name = 'datex'
                    df = df.reset_index()
                    df['feed_type'] = feed
                    amt_rows.append(df)
            # unit_price lives inside cost_sequence (concat of daily_amt + daily_price_seq)
            if 'cost_sequence' in feed_result:
                cost_seq_df = feed_result['cost_sequence']
                if isinstance(cost_seq_df, pd.DataFrame) and 'unit_price' in cost_seq_df.columns:
                    df = cost_seq_df[['unit_price']].copy()
                    df['feed_type'] = feed
                    df.index.name = 'datex'
                    df = df.reset_index()
                    price_rows.append(df)
        # Concatenate all
        amt_df = pd.concat(amt_rows, ignore_index=True) if amt_rows else pd.DataFrame()
        price_df = pd.concat(price_rows, ignore_index=True) if price_rows else pd.DataFrame()
        return amt_df, price_df
       

    def aggregate_feedcosts_from_dict(self):
        # Use feedcost_data.results to aggregate all feeds
        cost_frames = []
        concat_last_rows = []
        for feed in self.feed_type:
            feed_result = self.feedcost_data.results.get(feed)
            if feed_result and 'cost_sequence' in feed_result:
                df = feed_result['cost_sequence']
                if isinstance(df, pd.DataFrame) and 'daily cost' in df.columns:
                    col_name = f'{feed} daily cost'
                    feed_df = df.loc[:, ['daily cost']].rename(columns={'daily cost': col_name})
                    cost_frames.append(feed_df)
                    if not feed_df.empty:
                        last_row_cost = feed_df.iloc[[-1], :]
                        concat_last_rows.append(last_row_cost)
                    if 'daily_amt' in feed_result:
                        daily_amt_df = feed_result['daily_amt']
                        if isinstance(daily_amt_df, pd.DataFrame) and not daily_amt_df.empty:
                            last_row_amt = daily_amt_df.iloc[[-1], :]
                            # Rename columns to include feed name for clarity
                            last_row_amt = last_row_amt.rename(columns=lambda c: f'{feed} {c}')
                            concat_last_rows.append(last_row_amt)
                # else: skip
            # else: skip
        if concat_last_rows:
            combined_last_rows = pd.concat(concat_last_rows, axis=1)
            print("\nCombined last rows of cost_sequence and daily_amt for all feeds:")
            print(combined_last_rows)
        if not cost_frames:
            raise ValueError("No feed cost data found in feedcost_data.results.")
        tfc = pd.concat(cost_frames, axis=1)
        tfc['total feedcost'] = tfc.sum(axis=1)
        if concat_last_rows:
            tfc_details = pd.concat([r for r in concat_last_rows if r.shape[0] == 1], axis=0)
        else:
            tfc_details = pd.DataFrame()

        self.feed_cost_from_dict = tfc
        self.feed_cost_from_dict_details = tfc_details
        return self.feed_cost_from_dict, self.feed_cost_from_dict_details

    def aggregate_feedcosts_from_df(self, amt_df, price_df):
            """
            Aggregate feed costs using a single DataFrame approach.
            amt_df: DataFrame with columns [datex, feed_type, fresh_kg, group_a_kg, ...]
            price_df: DataFrame with columns [datex, feed_type, weight, unit_price, ...]
            Returns: (feedcost DataFrame, details DataFrame)
            """
            # Merge amount and price data on datex and feed_type
            merged = pd.merge(amt_df, price_df[['datex', 'feed_type', 'unit_price']], on=['datex', 'feed_type'], how='left')
            # Calculate daily cost for each group
            group_cols = [col for col in amt_df.columns if col.endswith('_kg')]
            for group in group_cols:
                merged[f'{group}_cost'] = merged[group] * merged['unit_price']
            # Sum across all groups for total feedcost per row
            merged['total_feedcost'] = merged[[f'{g}_cost' for g in group_cols]].sum(axis=1)
            # Pivot to get a DataFrame: index=datex, columns=feed_type, values=total_feedcost
            feedcost = merged.pivot_table(index='datex', columns='feed_type', values='total_feedcost', aggfunc='sum')
            feedcost['total feedcost'] = feedcost.sum(axis=1)
            # Details: last row for each feed_type
            details = merged.sort_values('datex').groupby('feed_type').tail(1)
            details = details.set_index(['feed_type', 'datex'])
            self.feedcost_from_df = feedcost
            self.feedcost_from_df_details = details
            return self.feedcost_from_df, self.feedcost_from_df_details

    def create_monthly_from_dict(self):
        fct = self.feedcost['total feedcost'].to_frame()
        fct.index = pd.to_datetime(fct.index)
        fct['year'] = fct.index.year
        fct['month'] = fct.index.month
        fct_monthly_from_dict   = fct
        fctm1_from_dict         = fct.groupby(['year','month']).agg({'total feedcost' : 'sum'})
        self.total_feedcost_monthly = fctm1_from_dict
        return self.total_feedcost_monthly

    def create_weekly_from_dict(self):
        fct = self.feedcost['total feedcost'].to_frame()
        fct.index = pd.to_datetime(fct.index)
        fct['week'] = fct.index.to_series().dt.to_period('W').apply(lambda r: r.start_time)
        fctw1_from_dict = fct.groupby('week').agg({'total feedcost': 'sum'}).reset_index()
        self.total_feedcost_weekly = fctw1_from_dict
        return self.total_feedcost_weekly

    def create_total_feedcost_monthly_from_df(self):
        """
        Compute monthly total feedcost using the DataFrame-based feedcost result.
        Returns a DataFrame with year, month, and total feedcost.
        """
        if self.feedcost_from_df is None:
            return None
        fct = self.feedcost_from_df['total feedcost'].to_frame()
        fct.index = pd.to_datetime(fct.index)
        fct['year'] = fct.index.year
        fct['month'] = fct.index.month
        fctm_df1 = fct
        fctm_df = fctm_df1.groupby(['year', 'month']).agg({'total feedcost': 'sum'})
        self.total_feedcost_monthly_from_df = fctm_df

        return self.total_feedcost_monthly_from_df


    def write_to_csv(self):
        self.feedcost                   .to_csv(r'E:\COWS\data\feed_data\feedcost_by_group\feedcost.csv')
        self.total_feedcost_details_last.to_csv(r'E:\COWS\data\feed_data\feedcost_by_group\total_feedcost_details_last.csv')
        self.total_feedcost_monthly     .to_csv(r'E:\COWS\data\feed_data\feedcost_by_group\total_feedcost_monthly.csv')
        self.total_feedcost_weekly      .to_csv(r'E:\COWS\data\feed_data\feedcost_by_group\total_feedcost_weekly.csv')
        # Write the DataFrame-based monthly result for comparison


        self.total_feedcost_monthly_from_df.to_csv(r'E:\COWS\data\feed_data\feedcost_by_group\total_feedcost_monthly_from_df.csv')
        self.feedcost_from_df_details   .to_csv(r'E:\COWS\data\feed_data\feedcost_by_group\total_feedcost_monthly_from_df.csv')



if __name__ == "__main__":
    obj = Feedcost_total()
    obj.load_and_process()