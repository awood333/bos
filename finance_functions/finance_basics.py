'''finance_functions.FinanceBasics.py'''
import os
import inspect
from pathlib import Path
import pandas as pd
from sql_db_related.neon_connect import get_engine

from feed_functions.feedcost_basics import FeedcostBasics


class FinanceBasics:
    def __init__(self, feedcost_basics=None):
        print(f"FinanceBasics instantiated by: {inspect.stack()[1].filename}")
        self.fc = feedcost_basics or FeedcostBasics()
        self.bkk1 = None
        self.startdate = None
        self.stopdate = None
        self.idx = None
        self.feed_cost_pivot = None
        self.cost_xfeed_pivot = None
        self.feedcost_pivot = None
        self.engine = get_engine()

    def load_and_process(self):
        with self.engine.connect() as conn:
                    bkk = pd.read_sql_table('bkk_bank', conn)
        bkk = bkk.drop(columns=['id'])
        
        self.bkk_farm_1 = bkk.loc["2025-01-01":, :].copy()
        self.bkk_farm_1 = self.bkk_farm_1.drop(columns=['credit', 'desc_3', 'desc_4', 'brahman'])
        self.bkk_farm_1 = self.bkk_farm_1[
            self.bkk_farm_1['desc_1'].notna()].copy()  #removes the missing rows (when entry is in 'credit')
        
        self.bkk_farm_2 = self.bkk_farm_1[
            self.bkk_farm_1['desc_1'] !='nonfarm'].copy()  #bkk x nonfarm
        
        self.bkk_nonfarm = self.bkk_farm_1[
            self.bkk_farm_1['desc_1'] =='nonfarm'].copy()
                
        self.bkk_farm_non_capex = self.bkk_farm_2[
            self.bkk_farm_2['capex'] != 'x'].copy().drop(columns=['capex'])
        
        self.bkk_farm_capex = self.bkk_farm_2[
            self.bkk_farm_2['capex'] == 'x'].copy()        
        
        self.bkk_farm_x_feed = self.bkk_farm_non_capex[
            self.bkk_farm_non_capex['desc_1'] != 'feed' ].copy()
        
        self.bkk_feed = self.bkk_farm_non_capex[
            self.bkk_farm_non_capex['desc_1'] == 'feed' ].copy()

        #datex cannot be the index - multiple values -- so use the datex col instead
        self.feed_cost_pivot = self.create_feed_pivot()
        self.cost_xfeed_pivot = self.create_cost_xfeed_pivot()
        
    def create_feed_pivot(self):
        df1 = self.bkk_feed.copy()
        df1['datex'] = pd.to_datetime(
            df1['year'].astype(str) + '-' + df1['month'].astype(str) + '-01'
        )
        df1 = df1.sort_values('datex')        
        
        
        df2 = pd.pivot_table(df1,
            index = 'datex',
            values = 'debit',
            columns= 'desc_2',
            aggfunc= 'sum'  )
 
            # Melt wide pivot -> tidy long format
        self.feed_cost_pivot = (
            df2.stack()
            .rename('value')
            .reset_index()          # columns: datex, desc_2, value
            )
        
        return self.feed_cost_pivot
        
        
    def create_cost_xfeed_pivot(self):
        df = self.bkk_farm_x_feed.copy()
        df['datex'] = pd.to_datetime(
            df['year'].astype(str) + '-' + df['month'].astype(str) + '-01'
        )
        df = df[df['desc_1'] != 'sale']

        self.cost_xfeed_pivot = (
            df.groupby(['datex', 'desc_1'], as_index=False)['debit']
            .sum()
            .rename(columns={'debit': 'value'})
        )
        return self.cost_xfeed_pivot  #longifies the cost data after grouping it by month (the datex line crams it together)



    
if __name__ == "__main__":
    obj=FinanceBasics()
    obj.load_and_process() 