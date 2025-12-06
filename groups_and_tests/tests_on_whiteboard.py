'''milk_functions.report_milk.Tests_on_whiteboard.py'''
import inspect
from datetime import timedelta
import pandas as pd
import openpyxl
# import numpy as np
# from scipy.optimize import curve_fit
from container import get_dependency


class TestsOnWhiteboard:
    def __init__(self):
        print(f"TestsOnWhiteboard instantiated by: {inspect.stack()[1].filename}")
        self.IUB = None
        self.FCB = None
        self.WBG  = None
        self.NRTL = None
        self.weekly_milk = None
        self.fullday_milk = None
        self.date_of_change = None
        self.date_of_change1 = None        
        self.last_date_before_change_feed = None
        self.start_date = None
        self.end_date = None
        self.testframe = None

        self.sick_cows = []
        self.wb_groups_on_dayofchange = None
        self.milking_days_on_dayofchange = None

        self.net_rev_group_A = None
        self.net_rev_group_B = None
        self.net_rev_group_C = None





        self.df1 = self.df2 = self.df3 = self.df4 = None
        self.comp = None
        self.averages_by_group = None
        self.all_cows_avgs = None
        self.testframe_group_A = None
        self.gross_profit_comp1 = None
        self.new_feedcost = None

    def load_and_process(self):
        self.IUB = get_dependency('insem_ultra_basics')
        self.FCB = get_dependency('feedcost_basics')
        self.WBG   = get_dependency('whiteboard_groups')
        self.NRTL = get_dependency('NetRevThisLactation_WB')
        
        self.date_of_change1 = "2025-09-27"  #NOTE  
        self.date_of_change = pd.to_datetime(self.date_of_change1, format='%Y-%m-%d')

        self.start_date     = self.date_of_change - timedelta(days=3)
        self.end_date       = '2025-10-19'
        self.last_date_before_change_feed = self.date_of_change - timedelta( days=1 )

        self.start_date     = pd.to_datetime(self.start_date,   format='%Y-%m-%d')
        self.end_date       = pd.to_datetime(self.end_date,     format='%Y-%m-%d')

        self.wb_groups_on_dayofchange = self.WBG.whiteboard_groups_specific_date

        self.net_rev_group_A = self.create_group_A_net_revenue_df()
        self.net_rev_group_B = self.create_group_B_net_revenue_df()
        self.net_rev_group_C = self.create_group_C_net_revenue_df()
        
        self.write_to_excel()
    

    def create_group_A_net_revenue_df(self):

        g1 = self.wb_groups_on_dayofchange.reset_index(drop=True)

        # set = Build an unordered collection of unique elements.
        groupA_ids = set(g1.loc[g1['group'] == 'A', 'WY_id'].astype(str))
        NDD = self.NRTL.nested_dict_by_date
        records = []
        for date, cows in NDD.items():
            for wy_id, cow_data in cows.items():
                wy_id_str = str(wy_id)
                if cow_data.get('group') == 'A' and wy_id_str in groupA_ids:
                    records.append({
                        'date': date,
                        'WY_id': str(wy_id),
                        'net_revenue': cow_data.get('net_revenue')
                    })
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'])
        pivot_df1 = df.pivot(index='date', columns='WY_id', values='net_revenue')
        self.net_rev_group_A  = pivot_df1.loc[self.start_date : self.end_date,:]

        return self.net_rev_group_A


    def create_group_B_net_revenue_df(self):

        g1 = self.wb_groups_on_dayofchange.reset_index(drop=True)

        # set = Build an unordered collection of unique elements.
        groupB_ids = set(g1.loc[g1['group'] == 'B', 'WY_id'].astype(str))
        NDD = self.NRTL.nested_dict_by_date
        records = []
        for date, cows in NDD.items():
            for wy_id, cow_data in cows.items():
                wy_id_str = str(wy_id)
                if cow_data.get('group') == 'B' and wy_id_str in groupB_ids:
                    records.append({
                        'date': date,
                        'WY_id': str(wy_id),
                        'net_revenue': cow_data.get('net_revenue')
                    })
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'])
        pivot_df1 = df.pivot(index='date', columns='WY_id', values='net_revenue')
        self.net_rev_group_B  = pivot_df1.loc[self.start_date : self.end_date,:]

        return self.net_rev_group_B


    def create_group_C_net_revenue_df(self):

        g1 = self.wb_groups_on_dayofchange.reset_index(drop=True)

        # set = Build an unordered collection of unique elements.
        groupC_ids = set(g1.loc[g1['group'] == 'C', 'WY_id'].astype(str))
        NDD = self.NRTL.nested_dict_by_date
        records = []
        for date, cows in NDD.items():
            for wy_id, cow_data in cows.items():
                wy_id_str = str(wy_id)
                if cow_data.get('group') == 'C' and wy_id_str in groupC_ids:
                    records.append({
                        'date': date,
                        'WY_id': str(wy_id),
                        'net_revenue': cow_data.get('net_revenue')
                    })
        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'])
        pivot_df1 = df.pivot(index='date', columns='WY_id', values='net_revenue')
        self.net_rev_group_C  = pivot_df1.loc[self.start_date : self.end_date,:]

        return self.net_rev_group_C


    

    def write_to_excel(self):

        with pd.ExcelWriter(
            r"F:\COWS\data\milk_data\tests\test_on_whiteboard_2025-09-27.xlsx",
            engine='openpyxl'
        ) as writer:
            if self.net_rev_group_A is not None:
                self.net_rev_group_A.to_excel(writer, sheet_name='group_A')
            if self.net_rev_group_B is not None:
                self.net_rev_group_B.to_excel(writer, sheet_name='group_B')
            if self.net_rev_group_C is not None:
                self.net_rev_group_C.to_excel(writer, sheet_name='group_C')



    
if __name__ == "__main__":
    obj=TestsOnWhiteboard()
    obj.load_and_process()     
