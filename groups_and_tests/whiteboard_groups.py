'''milk_functions.WhiteboardGroups.py'''
import inspect
import os
from pyexcel_ods import get_data
import pandas as pd
import json
pd.set_option('future.no_silent_downcasting', True)
import numpy as np
from container import get_dependency



class WhiteboardGroups:
    def __init__(self):

        print(f"WhiteboardGroups instantiated by: {inspect.stack()[1].filename}")
        
        self.ods_data = get_data('F:\\COWS\\data\\daily_milk.ods')

        self.MA = None
        self.MB = None
        self.BD = None
        self.SD= None
        self.alive_mask = None
        self.milk_aggregates =None
        self.fullday = None
        self.fullday_last = None
        self.tenday = None


        self.start_date = None
        self.date_range = None
        self.data_whiteboard = None
        self.sick_df = None
        self.groups_by_date_by_cow = None
        self.groups_matrix = None
        self.whiteboard_groups_dict = None
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
        self.SD         = get_dependency('status_data')
        self.alive_mask = self.SD.alive_ids_last
        self.fullday    = self.MA.fullday.copy()
        self.fullday_last    = self.MA.fullday.iloc[-1:, :].copy()

        self.tenday     = self.MA.tenday

        start_date      ='2025-09-01'
        stop_date       = self.fullday_last.index[0]  #.index[0] gets the first (and only) value from this Index
        self.date_range = pd.date_range(start_date, stop_date)


        def sheet_to_df(sheet):
            data = self.ods_data[sheet]
            df = pd.DataFrame(data)
            df = df.iloc[2:].reset_index(drop=True)
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            df.set_index(df.columns[0], inplace=True)
            df.index = df.index.fillna(0)
            df.index = df.index.astype(int)
            df.columns = pd.to_datetime(df.columns, errors='coerce')
            df = df.iloc[:55, :]
            return df
            
        self.fresh_df   = sheet_to_df('fresh')
        self.group_a_df = sheet_to_df('group_A')
        self.group_b_df = sheet_to_df('group_B')
        self.group_c_df = sheet_to_df('group_C')
        self.sick_df    = sheet_to_df('sick')


        


        # Now all self.fresh_df, self.group_a_df, ... have datetime columns
        self.groups_matrix, self.whiteboard_groups_dict = self.create_whiteboard_groups_dict()
        self.groups_by_date_by_cow = self.create_groups_by_date_by_cow()
        self.whiteboard_groups_specific_date, self.specific_date = self.create_whiteboard_groups_specific_date()
        self.whiteboard_groups_for_dailymilk = self.create_whiteboard_groups_for_dailymilk()
        self.whiteboard_groups_tenday = self.create_whiteboard_groups_tenday()
        self.write_to_csv()



    def create_whiteboard_groups_dict(self):
        #the group_df is the page from daily milk, so shape 55,xxx
        def get_ids_by_date(df):
            # If df is a DataFrame, process each column (date) separately
            if isinstance(df, pd.DataFrame):
                result = {}
                for col in df.columns:
                    ids = df[col]
                    # Only process if ids is 1D (Series, list, tuple)
                    if isinstance(ids, (pd.Series, list, tuple)):
                        ids_numeric = pd.to_numeric(ids, errors='coerce')
                        ids_clean = ids_numeric.dropna()
                        result[str(col)] = ids_clean.astype(int).tolist() if not ids_clean.empty else []
                    else:
                        # If not 1D, skip or store as empty
                        result[str(col)] = []
                return result
            else:
                # Fallback: treat as a single column/list
                if isinstance(df, (pd.Series, list, tuple)):
                    ids_numeric = pd.to_numeric(df, errors='coerce')
                    ids_clean = ids_numeric.dropna()
                    return {'unknown': ids_clean.astype(int).tolist() if not ids_clean.empty else []}
                else:
                    return {'unknown': []}

        whiteboard_groups_dict = {
            "fresh_ids": get_ids_by_date(self.fresh_df),
            "group_A_ids": get_ids_by_date(self.group_a_df),
            "group_B_ids": get_ids_by_date(self.group_b_df),
            "group_C_ids": get_ids_by_date(self.group_c_df),
            "sick_ids": get_ids_by_date(self.sick_df)
        }
        self.whiteboard_groups_dict = whiteboard_groups_dict

        # Build the main groups_matrix DataFrame for reuse
        wy_ids = [str(int(wy)) for wy in self.BD['WY_id']]
        groups_matrix = pd.DataFrame(index=self.date_range, columns=wy_ids)  #.strftime('%Y-%m-%d')
        for group_label, group_dict in [
            ('F', whiteboard_groups_dict.get('fresh_ids', {})),
            ('A', whiteboard_groups_dict.get('group_A_ids', {})),
            ('B', whiteboard_groups_dict.get('group_B_ids', {})),
            ('C', whiteboard_groups_dict.get('group_C_ids', {})),
            ('sick', whiteboard_groups_dict.get('sick_ids', {}))
        ]:
            for date_str, ids in group_dict.items():
                for wy_id in ids:
                    if wy_id in groups_matrix.columns:
                        groups_matrix.at[date_str, wy_id] = group_label

        # Sort columns numerically
        sorted_cols = sorted([int(col) for col in groups_matrix.columns])
        groups_matrix = groups_matrix[[str(col) for col in sorted_cols]]

        # Apply sick value replacement 
        groups_matrix = self.replace_sick_with_previous(groups_matrix)
        self.groups_matrix = groups_matrix  # Store for reuse

        return self.groups_matrix, self.whiteboard_groups_dict
    

    def replace_sick_with_previous(self, df):
        df = df.copy()
        for col in df.columns:
            col_vals = df[col].tolist()
            for i in range(1, len(col_vals)):
                if isinstance(col_vals[i], str) and col_vals[i].lower() == 'sick':
                    col_vals[i] = col_vals[i-1]
            df[col] = col_vals
        return df
                    

    def replace_nan_in_dict(self, obj):
        if isinstance(obj, dict):
            return {str(k): self.replace_nan_in_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_nan_in_dict(v) for v in obj]
        elif obj is None or (isinstance(obj, float) and math.isnan(obj)) or obj is pd.NA:
            return "NaN"
        else:
            return obj

    def save_model_groups_json(self, filepath="F:\\COWS\\data\\status\\whiteboard_groups.json"):
        # Convert DataFrames to dicts
        dict_to_save = {k: v.to_dict() if hasattr(v, "to_dict") else v for k, v in self.whiteboard_groups_dict.items()}
        # Replace NaN/NA
        cleaned_dict = self.replace_nan_in_dict(dict_to_save)
        # Save as JSON
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(cleaned_dict, f, indent=2, default=str)

          

    def create_groups_by_date_by_cow(self):
        wbg= self.groups_matrix.T
        # alive_mask is a list of cow IDs matching columns, not index
        # Select columns using alive_mask, ensure types match
        try:
            alive_mask_int = [int(x) for x in self.alive_mask]
            wbg.columns = wbg.columns.astype(int)
        except Exception:
            alive_mask_int = [str(x) for x in self.alive_mask]
            wbg.columns = wbg.columns.astype(str)
        # Only select columns that exist in wbg
        available_cols = [col for col in alive_mask_int if col in wbg.columns]
        self.groups_by_date_by_cow = wbg.loc[:, available_cols]
        return self.groups_by_date_by_cow

    def create_whiteboard_groups_specific_date(self):
        '''#returns df with the WY_id and group on spec date'''

        self.specific_date = '2025-09-27'
        specific_date2 = pd.to_datetime(self.specific_date).strftime('%Y-%m-%d')
        # Get the group assignments for this date
        row = self.groups_matrix.loc[specific_date2]
        # Convert to DataFrame for consistency
        group_df = row.reset_index()
        group_df.columns = ['WY_id', 'group']
        group_df = group_df.dropna(subset=['group'])
        self.whiteboard_groups_specific_date = group_df 
        return self.whiteboard_groups_specific_date, self.specific_date

    def create_whiteboard_groups_for_dailymilk(self):
        # Use the most recent date from milk aggregates
        specific_date = self.MA.fullday_lastdate.index[0]
        specific_date1 = pd.to_datetime(specific_date, format='%Y-%m-%d')
        specific_date2 = specific_date1.strftime('%Y-%m-%d')

        # Use the loaded DataFrames directly
        # Each DataFrame column is a Timestamp, so match by Timestamp
        date_col = pd.to_datetime(specific_date2)

        def get_group_df(group_df, group_label):
            if date_col in group_df.columns:
                group_df = group_df.iloc[:70][date_col].copy().to_frame()
                group_df['group'] = group_label
                group_df[date_col] = group_df[date_col].replace(r'^\s*$', np.nan, regex=True)
                group_df = group_df.dropna(subset=[date_col])
                return group_df
            else:
                return None

        #indentation is correct
        f1 = get_group_df(self.fresh_df, 'F')
        a1 = get_group_df(self.group_a_df, 'A')
        b1 = get_group_df(self.group_b_df, 'B')
        c1 = get_group_df(self.group_c_df, 'C')
        s1 = get_group_df(self.sick_df, 'sick')

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

        def get_group_slice(group_df, group_label):
            # Get the most recent column (last date)
            col = group_df.columns[-1]
            ids = group_df.iloc[:70, -1].copy()
            # Convert to string WY_ids
            wy_ids = [str(int(float(x))) for x in pd.to_numeric(ids, errors='coerce') if pd.notna(x)]
            # Filter td2 for these WY_ids
            filtered = td2[td2['WY_id'].astype(str).isin(wy_ids)].copy()
            if not filtered.empty:
                filtered.loc[:, 'group'] = group_label
            return filtered

        f3 = get_group_slice(self.fresh_df, 'F')
        a3 = get_group_slice(self.group_a_df, 'A')
        b3 = get_group_slice(self.group_b_df, 'B')
        c3 = get_group_slice(self.group_c_df, 'C')
        s1 = get_group_slice(self.sick_df, 'sick')

        # sick group special ฉีดยา group_label)
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
        self.whiteboard_groups_tenday       .to_csv('F:\\COWS\\data\\groups_and_tests\\whiteboard_groups_tenday.csv')
        self.whiteboard_groups_for_dailymilk.to_csv('F:\\COWS\\data\\groups_and_tests\\whiteboard_groups_for_dailymilk.csv')
        self.groups_by_date_by_cow          .to_csv('F:\\COWS\\data\\groups_and_tests\\whiteboard_groups_by_date_by_cow.csv')
        self.whiteboard_groups_specific_date.to_csv('F:\\COWS\\data\\groups_and_tests\\whiteboard_groups_specific_date.csv')

        # Replace NaN/NA in model_groups_dict before saving as JSON
        cleaned_dict = self.replace_nan_in_dict(self.whiteboard_groups_dict)
        with open("F:\\COWS\\data\\status\\whiteboard_groups_dict.json", 'w', encoding='utf-8') as f:
            json.dump(cleaned_dict, f, indent=2, default=str)           
    

if __name__ == "__main__":
    obj=WhiteboardGroups()
    obj.load_and_process()      