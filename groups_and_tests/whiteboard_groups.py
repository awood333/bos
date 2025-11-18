'''milk_functions.WhiteboardGroups.py'''
import inspect
from datetime import datetime
import os
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import numpy as np
from container import get_dependency



class WhiteboardGroups:
    def __init__(self):

        print(f"WhiteboardGroups instantiated by: {inspect.stack()[1].filename}")
        
        self.MA = None
        self.MB = None
        self.BD = None
        self.SD2= None
        self.alive_mask = None
        self.milk_aggregates =None
        self.fullday = None
        self.fullday_last = None
        self.tenday = None
        self.data_whiteboard = None
        self.sick_df = None
        self.groups_by_date_by_cow = None
        self.whiteboard_groups_df = None
        self.whiteboard_groups_specific_date = None
        self.whiteboard_groups_for_dailymilk = None
        self.specific_date = None
        self.whiteboard_groups_tenday = None
        self.fresh_df = None
        self.group_a_df = None
        self.group_b_df = None
        self.group_c_df = None
        

    def load_and_process(self):

        self.MB         = get_dependency('milk_basics')
        self.BD         = self.MB.bd.copy()
        self.MA         = get_dependency('milk_aggregates')
        self.SD2        = get_dependency('status_data2')
        self.alive_mask = self.SD2.alive_mask['WY_id'].astype(str).reset_index(drop=True)

        self.fullday    = self.MA.fullday.copy()
        self.fullday_last = self.MA.fullday.iloc[-1:, :].copy()
        self.tenday     = self.MA.tenday

        # Load group CSVs and ensure columns are datetime
        base_path = 'F:\\COWS\\data\\milk_data\\wb_groups'
        group_files = {
            'fresh_df'  : 'fresh.csv',
            'group_a_df': 'group_A.csv',
            'group_b_df': 'group_B.csv',
            'group_c_df': 'group_C.csv',
            'sick_df'   : 'sick.csv'
        }
        for attr, fname in group_files.items():
            df          = pd.read_csv(os.path.join(base_path, fname), index_col='index')
            df.columns  = pd.to_datetime(df.columns, errors='coerce')
            setattr(self, attr, df)

        # Now all self.fresh_df, self.group_a_df, ... have datetime columns

        self.whiteboard_groups_df = self.create_whiteboard_groups_df()
        self.groups_by_date_by_cow = self.create_groups_by_date_by_cow()
        self.whiteboard_groups_specific_date, self.specific_date = self.create_whiteboard_groups_specific_date()
        self.whiteboard_groups_for_dailymilk = self.create_whiteboard_groups_for_dailymilk()
        self.whiteboard_groups_tenday = self.create_whiteboard_groups_tenday()
        self.write_to_csv()
        
            
    def create_whiteboard_groups_df(self, start_date='2025-09-01'):
        # Get all dates from start_date onward that exist in the group DataFrames
        #Build all_dates from the union of all group DataFrame columns
        all_dates = pd.to_datetime(
            sorted(
                set(self.group_a_df.columns)
                | set(self.group_b_df.columns)
                | set(self.group_c_df.columns)
                | set(self.fresh_df.columns)
                | set(self.sick_df.columns)
            ),
            errors="coerce"
        )
        date_range = all_dates[all_dates >= pd.to_datetime(start_date)]
        wy_ids = [str(int(wy)) for wy in self.BD['WY_id']]
        result = pd.DataFrame(index=date_range.strftime('%Y-%m-%d'), columns=wy_ids)

        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            for df, label in [
                (self.fresh_df  , 'F'),
                (self.group_a_df, 'A'),
                (self.group_b_df, 'B'),
                (self.group_c_df, 'C'),
                (self.sick_df   , 'Sick')
            ]:
                if date in df.columns:
                    ids = df[date].dropna()
                    
                    # idea here is to get rid of blank cells that are not NaN --- Replace 'none' and blanks with np.nan
                    ids = ids.replace(['none', '', 'None'], np.nan)
                    ids = pd.to_numeric(ids, errors='coerce')  # ensures dtype is float, all non-numeric become NaN
                    ids = ids.dropna()

                    # Convert to numeric, drop invalids
                    wy_ids_numeric = pd.to_numeric(ids, errors='coerce').dropna().astype(int)
                    for wy_id in wy_ids_numeric:
                        wy_id_str = str(wy_id)
                        if wy_id_str in result.columns:
                            result.at[date_str, wy_id_str] = label

        # Sort columns as integers, then convert back to string
        sorted_cols = sorted([int(col) for col in result.columns])
        result = result[[str(col) for col in sorted_cols]]

        self.whiteboard_groups_df = result
        return self.whiteboard_groups_df

    def create_groups_by_date_by_cow(self):
        WBG1= self.whiteboard_groups_df.T
        self.groups_by_date_by_cow = WBG1.loc[self.alive_mask].T

        return self.groups_by_date_by_cow

        


    def create_whiteboard_groups_specific_date(self):
        # Just select the row for the specific date from self.whiteboard_groups_df
        self.specific_date = '2025-09-27'
        specific_date2 = pd.to_datetime(self.specific_date).strftime('%Y-%m-%d')
        # Get the group assignments for this date
        row = self.whiteboard_groups_df.loc[specific_date2]
        # Convert to DataFrame for consistency
        df = row.reset_index()
        df.columns = ['WY_id', 'group']
        df = df.dropna(subset=['group'])
        self.whiteboard_groups_specific_date = df
        return self.whiteboard_groups_specific_date, self.specific_date

    def create_whiteboard_groups_for_dailymilk(self):
        # Use the most recent date from milk aggregates
        specific_date = self.MA.fullday_lastdate.index[0]
        specific_date1 = pd.to_datetime(specific_date, format='%Y-%m-%d')
        specific_date2 = specific_date1.strftime('%Y-%m-%d')

        # Use the loaded DataFrames directly
        # Each DataFrame column is a Timestamp, so match by Timestamp
        date_col = pd.to_datetime(specific_date2)

        def get_group_df(df, label):
            if date_col in df.columns:
                group_df = df.iloc[:70][date_col].copy().to_frame()
                group_df['group'] = label
                group_df[date_col] = group_df[date_col].replace(r'^\s*$', np.nan, regex=True)
                group_df = group_df.dropna(subset=[date_col])
                return group_df
            else:
                return None

        f1 = get_group_df(self.fresh_df, 'F')
        a1 = get_group_df(self.group_a_df, 'A')
        b1 = get_group_df(self.group_b_df, 'B')
        c1 = get_group_df(self.group_c_df, 'C')
        s1 = get_group_df(self.sick_df, 'Sick')

        frames = [f1, a1, b1, c1, s1]
        d1 = pd.concat(frames, axis=0)
        d2 = d1.reset_index(drop=True)

        # Convert the date column to integer (ignore errors, drop NaN)
        d2[date_col] = pd.to_numeric(d2[date_col], errors='coerce').dropna().astype(int)

        self.whiteboard_groups_for_dailymilk = d2
        return self.whiteboard_groups_for_dailymilk

    # tenday should come from here (whiteboard) not from model_groups
    def create_whiteboard_groups_tenday(self):

        td1 = self.tenday.copy()
        # filters out the bottom row (avg/total) and gets the slice for WY_id and relevant columns
        td2 = td1.iloc[:-1, [0, 11, 12, 13, 14, 15]].copy()
        td2.columns.values[0] = 'WY_id'

        def get_group_slice(df, label):
            # Get the most recent column (last date)
            col = df.columns[-1]
            ids = df.iloc[:70, -1].copy()
            # Convert to string WY_ids
            wy_ids = [str(int(float(x))) for x in pd.to_numeric(ids, errors='coerce') if pd.notna(x)]
            # Filter td2 for these WY_ids
            group_df = td2[td2['WY_id'].astype(str).isin(wy_ids)].copy()
            group_df.loc[:, 'group'] = label
            return group_df

        f3 = get_group_slice(self.fresh_df, 'F')
        a3 = get_group_slice(self.group_a_df, 'A')
        b3 = get_group_slice(self.group_b_df, 'B')
        c3 = get_group_slice(self.group_c_df, 'C')

        # Sick group (special label)
        s1 = self.sick_df.iloc[:70, -1].copy()
        s2 = [str(int(float(x))) for x in pd.to_numeric(s1, errors='coerce') if pd.notna(x)]
        s3 = td2[td2['WY_id'].astype(str).isin(s2)].copy()
        if not s3.empty:
            s3.loc[:, 'group'] = 'ฉีดยา'

        frames = [f3, a3, b3, c3]
        if not s3.empty:
            frames.append(s3)

        d1 = pd.concat(frames, axis=0)
        d1['avg'] = d1['avg'].astype(float)
        d2 = d1.sort_values('avg', ascending=False)
        d3 = d2.reset_index(drop=True)

        self.whiteboard_groups_tenday = d3
        return self.whiteboard_groups_tenday



    
    def write_to_csv(self):
        self.whiteboard_groups_df       .to_csv('F:\\COWS\\data\\milk_data\\groups\\whiteboard_groups_df.csv')
        self.whiteboard_groups_tenday   .to_csv('F:\\COWS\\data\\milk_data\\groups\\whiteboard_groups_tenday.csv')
        self.whiteboard_groups_specific_date.to_csv(f'F:\\COWS\\data\\milk_data\\groups\\whiteboard_groups_specific_date_{self.specific_date}.csv')
        self.whiteboard_groups_for_dailymilk.to_csv('F:\\COWS\\data\\milk_data\\groups\\whiteboard_groups_for_dailymilk.csv')
    
if __name__ == "__main__":
    obj=WhiteboardGroups()
    obj.load_and_process()      