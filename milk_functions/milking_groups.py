'''milk_functions.milking_groups.py'''
import inspect
import pandas as pd
from pyexcel_ods import get_data

from milk_functions.milkaggregates import MilkAggregates

class MilkingGroups:
    def __init__(self, milk_aggregates=None):
        
        print(f"MilkingGroups instantiated by: {inspect.stack()[1].filename}")
        MA = milk_aggregates or MilkAggregates()
        
        self.fullday    = MA.fullday.iloc[-1:,:].copy()
        self.tenday     = MA.tenday


        data = get_data('F:\\COWS\\data\\daily_milk.ods')
        
        group_a_data = data.get('group A', [])
        group_b_data = data.get('group B', [])
        sick_data = data.get('sick', [])
        
        self.group_a_df = pd.DataFrame(group_a_data[1:]  , columns=group_a_data[0]).set_index('index')
        self.group_b_df = pd.DataFrame(group_b_data[1:]  , columns=group_b_data[0]).set_index('index')
        
        if (sick_data
        and len(sick_data) > 1 
        and isinstance(sick_data[0], list)
        ):
            valid_rows = [row for row in sick_data[1:] if len(row) 
                          == len(sick_data[0])]
            
            if valid_rows:
                self.sick_df    = pd.DataFrame(valid_rows , columns=sick_data[0]).set_index('index')
            else:
                self.sick_df = pd.DataFrame(columns=self.group_a_df.columns)

        else:
            self.sick_df = pd.DataFrame(columns=self.group_a_df.columns)
        
        self.sick_df.index.name = 'index'
        
        self.milking_groups = self.create_milking_groups()
        self.write_to_csv()
        
        
        
    def create_milking_groups(self):
        
        td1 = self.tenday.copy()
        
        # filters out the bottom 2 rows (avg and total) and gets the slice starting with col[9] 
        # (the 10th col and the last milking record before 'avg') thru 'expected bdate'
        td2 = td1.iloc[:-2,[0,11,12,13,14,15]].copy()
        td2.columns.values[0] = 'WY_id'
        
        # a1 filters for the most recent col of group_a
        a1 = self.group_a_df.iloc[:60,-1].copy()
        # str(int(float)) ensures that the dtype of a1 is string
        a2 = [str(int(float(x))) for x in a1 if pd.notna(x)]
        # a3 ensures that the index of td2 is string and masks to get only the group_a WY_ids
        a3 = td2 [td2['WY_id'].astype(str).isin(a2)].copy()
        # creates a new col 'group' and assigns 'A' to each row
        a3.loc[:, 'group'] = 'A'
        
        b1 = self.group_b_df.iloc[:60,-1].copy()
        b2 = [str(int(float(x))) for x in b1 if pd.notna(x)]
        b3 = td2[td2['WY_id'].astype(str).isin(b2)].copy()
        b3.loc[:, 'group'] = 'B'
        
        c1 = self.sick_df.iloc[:60,-1].copy()
        c2 = [str(int(float(x))) for x in pd.to_numeric(c1, 
                errors='coerce') if pd.notna(x)]
        c3 = td2[td2['WY_id'].astype(str).isin(c2)].copy()
        if not c3.empty:        
            c3.loc[:,'group'] = 'ฉีดยา'
        
        frames = [a3, b3]
        if not c3.empty:
            frames.append(c3)
        d1 = pd.concat(frames, axis=0)
        d2 = d1.sort_values('avg', ascending=False )
        d3=d2.reset_index(drop=True)
        # d4 = d3.rename(columns={'WY_id': 'WY_id_3'})
        # d5 = d3.iloc[:,[0,5,6,7]].copy()
        
        self.milking_groups = d3
            
        return self.milking_groups
    
    def write_to_csv(self):
        self.milking_groups.to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\milking_groups.csv')
    
    
if __name__ == "__main__":
    MilkingGroups()