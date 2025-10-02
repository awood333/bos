'''milk_functions.milking_groups.py'''
import inspect
import pandas as pd
from pyexcel_ods import get_data

from milk_functions.milk_aggregates import MilkAggregates

class MilkingGroups:
    def __init__(self, milk_aggregates=None):
        
        print(f"MilkingGroups instantiated by: {inspect.stack()[1].filename}")
        
        
        MA = milk_aggregates or MilkAggregates()
        
        self.fullday    = MA.fullday.iloc[-1:,:].copy()
        self.tenday     = MA.tenday

        self.data       = get_data('F:\\COWS\\data\\daily_milk.ods')
        self.sick_df    = self.parse_sick_data()
        
        self.milking_groups = self.create_milking_groups()
        self.write_to_csv()
        
        
        
    def parse_sick_data(self):
        
        sick_data    = self.data.get('sick', [])
        if (sick_data
            and len(sick_data) > 1 
            and isinstance(sick_data[0], list)
            ):
                valid_rows = [row for row in sick_data[1:] if len(row) 
                            == len(sick_data[0])]
                
                if valid_rows:
                    sick_df    = pd.DataFrame(valid_rows , columns=sick_data[0]).set_index('index')
                else:
                    sick_df = pd.DataFrame(columns=self.group_a_df.columns)
            
                sick_df.index.name = 'index'
                self.sick_df = sick_df
                
        return self.sick_df
        
        
    def create_milking_groups(self):
        
        fresh_data   = self.data.get('fresh', [])
        group_a_data = self.data.get('group A', [])
        group_b_data = self.data.get('group B', [])
        group_c_data = self.data.get('group C', [])  
        self.fresh_df   = pd.DataFrame(fresh_data[1:]    , columns=fresh_data[0])  .set_index('index')
        self.group_a_df = pd.DataFrame(group_a_data[1:]  , columns=group_a_data[0]).set_index('index')
        self.group_b_df = pd.DataFrame(group_b_data[1:]  , columns=group_b_data[0]).set_index('index')
        self.group_c_df = pd.DataFrame(group_c_data[1:]  , columns=group_c_data[0]).set_index('index')        

        td1 = self.tenday.copy()
        
        # filters out the bottom 2 rows (avg and total) and gets the slice starting with col[9] 
        # (the 10th col and the last milking record before 'avg') thru 'expected bdate'
        td2 = td1.iloc[:-1,[0,11,12,13,14,15]].copy()
        td2.columns.values[0] = 'WY_id'
        
        # fresh1 filters for the most recent col of group_fresh
        f1 = self.fresh_df.iloc[:70,-1].copy()

        # str(int(float)) ensures that the dtype of fresh1 is string
        f2 = [str(int(float(x))) for x in f1 if pd.notna(x)]

        # f3 ensures that the index of td2 is string and masks to get only the group_f WY_ids
        f3 = td2 [td2['WY_id'].astype(str).isin(f2)].copy()

        # creates a new col 'group' and assigns 'F' to each row
        f3.loc[:, 'group'] = 'F'

        a1 = self.group_a_df.iloc[:70,-1].copy()
        a2 = [str(int(float(x))) for x in a1 if pd.notna(x)]
        a3 = td2 [td2['WY_id'].astype(str).isin(a2)].copy()        
        a3.loc[:, 'group'] = 'A'
        
        b1 = self.group_b_df.iloc[:70,-1].copy()
        b2 = [str(int(float(x))) for x in b1 if pd.notna(x)]
        b3 = td2[td2['WY_id'].astype(str).isin(b2)].copy()
        b3.loc[:, 'group'] = 'B'
        
        c1 = self.group_c_df.iloc[:70,-1].copy()
        c2 = [str(int(float(x))) for x in c1 if pd.notna(x)]
        c3 = td2[td2['WY_id'].astype(str).isin(c2)].copy()
        c3.loc[:, 'group'] = 'C'        
        
        s1 = self.sick_df.iloc[:70,-1].copy()
        s2 = [str(int(float(x))) for x in pd.to_numeric(s1, 
                errors='coerce') if pd.notna(x)]
        s3 = td2[td2['WY_id'].astype(str).isin(s2)].copy()
        if not s3.empty:        
            s3.loc[:,'group'] = 'ฉีดยา'
        
        frames = [f3, a3, b3, c3]
        if not s3.empty:
            frames.append(s3)

        d1 = pd.concat(frames, axis=0)
        d1['avg'] = d1['avg'].astype(float)
        d2 = d1.sort_values('avg', ascending=False )
        d3=d2.reset_index(drop=True)

        
        self.milking_groups = d3
            
        return self.milking_groups
    
    def write_to_csv(self):
        self.milking_groups.to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\milking_groups.csv')
    
    
# if __name__ == "__main__":
#     MilkingGroups()