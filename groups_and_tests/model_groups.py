'''milk_functions\\model_groups.py'''
import inspect
import pandas as pd
import json
import math

from container import get_dependency


class ModelGroups:
    def __init__(self):

        print(f"ModelGroups instantiated by: {inspect.stack()[1].filename}")

        self.SD = None
        self.WD = None
        # self.MG = None
        self.IUB = None
        self.IUD = None
        self.MB = None
        self.DR = None
        self.DRM = None
        self.startdate = None
        self.herd_daily = None
        self.whiteboard_groups = None
        self.fresh_ids = None
        self.group_A_ids = None
        self.group_B_ids = None
        self.group_C_ids = None
        self.groups_by_date_by_cow = None
        self.all_groups_count = None
        self.model_groups_dict = None           
        self.model_groups_lastrow = None
        self.all_groups_count_monthly = None

    def load_and_process(self):

        self.SD = get_dependency('statusData')
        self.WD = get_dependency('wet_dry')
        self.IUB= get_dependency('insem_ultra_basics')
        self.IUD= get_dependency('insem_ultra_data')
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        
        self.DRM = self.DR.date_range_monthly_data
        self.startdate = self.DR.startdate
        self.herd_daily = self.SD.herd_daily

        [self.fresh_ids, self.group_A_ids, 
         self.group_B_ids, self.group_C_ids, 
         self.all_groups_count]     = self.create_model_groups()
        
        self.groups_by_date_by_cow = self.create_model_groups_df()
        
        self.model_groups_dict      = self.create_model_groups_dict()
        self.model_groups_lastrow   = self.get_model_groups_lastrow()

        self.all_groups_count_monthly = self.create_monthly()
        self.write_to_csv()
        self.save_model_groups_json()
        

    def create_model_groups (self):

        # startdatex = pd.to_datetime('2025-12-04')
        
        wet1a = self.WD.wdd.loc[self.startdate:, :]
        wetT = wet1a.T
        alive_mask = self.SD.alive_ids.astype(int)

        preg = self.IUD.all_preg
        pregnant_mask1 = preg[['WY_id', 'status']]
        pregnant_mask2 = pregnant_mask1.loc[pregnant_mask1['status'] == 'M'].reset_index(drop=True)
        pregnant_mask = pd.Series(pregnant_mask2['WY_id'])



        wet1b = wetT.loc[alive_mask]
        wet = wet1b.T

        milk1 = self.MB.data['milk'].loc[self.startdate:, :]
        

        fresh,      groupA,         groupB,         groupC          = [],[],[],[]
        fresh_ids1, groupA_ids1,    groupB_ids1,    groupC_ids1     = [],[],[],[]
        fresh_ids,  groupA_ids,     groupB_ids,     groupC_ids      = [],[],[],[]
        F_ids,      A_ids,          B_ids,          C_ids           = [],[],[],[]
        
        freshx,     groupAx,        groupBx,        groupCx         = 0,0,0,0


                
        for date in wet.index:
            date1   = pd.to_datetime(date)
            j1 = 0
            
            for i in wet.columns:
                j1 = wet.loc[date1, i]
                m1 = milk1.loc[date1, str(i)]
                
                if not pd.isna(m1):
                    m1 = milk1[str(i)].rolling(window=7).mean().loc[date1] #7 day MAvg
                    days1 = j1
                    # j = int(j1) - 1

                    if 0 < days1<= 21 :
                        freshx += 1
                        F_ids = i

                    elif (days1 >21 ) and m1 >= 12:   #140 days=20wks
                        groupAx += 1
                        A_ids = i

                    elif (days1 > 21) and (m1 < 12):
                        if i in pregnant_mask.values:
                            groupCx += 1
                            C_ids = i
                        else:
                            groupBx += 1
                            B_ids = i
                                        
                    
                
                if F_ids:    
                    fresh_ids1.append( F_ids )               
                
                if A_ids:    
                    groupA_ids1.append( A_ids )
                
                if B_ids:                    
                    groupB_ids1.append( B_ids )

                if C_ids:                    
                    groupC_ids1.append( C_ids )

                F_ids, A_ids, B_ids, C_ids = [],[],[],[]
                
            fresh.  append( [date1, freshx] )
            groupA. append( [date1, groupAx] )
            groupB. append( [date1, groupBx] )
            groupC. append( [date1, groupCx] )
            
            fresh_ids   .append([ date1] + fresh_ids1)
            groupA_ids  .append([ date1] + groupA_ids1)
            groupB_ids  .append([ date1] + groupB_ids1)
            groupC_ids  .append([ date1] + groupC_ids1)

            freshx  = 0            
            groupAx = 0
            groupBx = 0
            groupCx = 0

            fresh_ids1, groupA_ids1, groupB_ids1, groupC_ids1 = [],[],[],[]
            

        fresh_count   = pd.DataFrame(fresh, columns=['date', 'fresh'])            
        group_A_count = pd.DataFrame(groupA, columns=['date', 'groupA'])
        group_B_count = pd.DataFrame(groupB, columns=['date', 'groupB'])
        group_C_count = pd.DataFrame(groupC, columns=['date', 'groupC'])

        fresh_count   = fresh_count.set_index('date', drop=True)
        group_A_count = group_A_count.set_index('date', drop=True)
        group_B_count = group_B_count.set_index('date', drop=True)
        group_C_count = group_C_count.set_index('date', drop=True)

        self.all_groups_count = pd.concat((fresh_count, group_A_count, group_B_count, group_C_count), axis=1)
        self.all_groups_count['total count'] = self.all_groups_count.sum(axis=1)
        
        self.fresh_ids   = pd.DataFrame.from_records(fresh_ids).set_index(0)
        self.group_A_ids = pd.DataFrame.from_records(groupA_ids).set_index(0)
        self.group_B_ids = pd.DataFrame.from_records(groupB_ids).set_index(0)
        self.group_C_ids = pd.DataFrame.from_records(groupC_ids).set_index(0)
        
        return [self.fresh_ids, self.group_A_ids, self.group_B_ids,
                self.group_C_ids, self.all_groups_count]


    def create_model_groups_df(self, start_date=None):
        """
        Creates a DataFrame with WY_ids as columns and dates as index,
        with group labels ("F", "A", "B", "C") as values.
        Does not modify or replace create_model_groups.
        """
        # Use start_date if provided, else use self.startdate
        if start_date is None:
            start_date = self.startdate

        # Get all dates from all_groups_count index (already filtered by startdate)
        all_dates = pd.to_datetime(self.all_groups_count.index)
        wy_ids = [str(int(wy)) for wy in self.SD.alive_ids]

        # Prepare empty DataFrame
        result = pd.DataFrame(index=all_dates.strftime('%Y-%m-%d'), columns=wy_ids)

        # Helper: assign group label for each WY_id per date
        for date in all_dates:
            date_str = date.strftime('%Y-%m-%d')
            for df, label in [
                (self.fresh_ids, "F"),
                (self.group_A_ids, "A"),
                (self.group_B_ids, "B"),
                (self.group_C_ids, "C")
            ]:
                if date in df.index:
                    ids = df.loc[date].dropna()
                    for wy_id in ids:
                        wy_id_str = str(int(wy_id))
                        if wy_id_str in result.columns:
                            result.at[date_str, wy_id_str] = label

        # Sort columns numerically
        sorted_cols = sorted([int(col) for col in result.columns])
        result = result[[str(col) for col in sorted_cols]]

        self.groups_by_date_by_cow = result
        return self.groups_by_date_by_cow
    
    
    def create_model_groups_dict(self):
        def df_ids_to_dict(df):
            return {
                str(date): [str(id) for id in df.loc[date].dropna().values]
                for date in df.index
            }

        self.model_groups_dict = {
            "fresh_ids": df_ids_to_dict(self.fresh_ids),
            "group_A_ids": df_ids_to_dict(self.group_A_ids),
            "group_B_ids": df_ids_to_dict(self.group_B_ids),
            "group_C_ids": df_ids_to_dict(self.group_C_ids)
        }
        return self.model_groups_dict


    def replace_nan_in_dict(self, obj):
        if isinstance(obj, dict):
            return {str(k): self.replace_nan_in_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.replace_nan_in_dict(v) for v in obj]
        elif obj is None or (isinstance(obj, float) and math.isnan(obj)) or obj is pd.NA:
            return "NaN"
        else:
            return obj

    def save_model_groups_json(self, filepath="F:\\COWS\\data\\status\\model_groups_ids_dict.json"):
        # Convert DataFrames to dicts
        dict_to_save = {k: v.to_dict() if hasattr(v, "to_dict") else v for k, v in self.model_groups_dict.items()}
        # Replace NaN/NA
        cleaned_dict = self.replace_nan_in_dict(dict_to_save)
        # Save as JSON
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(cleaned_dict, f, indent=2, default=str)

            
        
    def get_model_groups_lastrow(self):
        f1=self.fresh_ids.iloc[-1:,:].copy()
        a1=self.group_A_ids.iloc[-1:,:].copy()
        b1=self.group_B_ids.iloc[-1:,:].copy()
        c1=self.group_C_ids.iloc[-1:,:].copy()

        f=f1.T
        f.columns = ["WY_id"]
        f['group'] = "F"

        
        a=a1.T
        a.columns = ["WY_id"]
        a['group'] = "A"

        
        b=b1.T
        b.columns = ["WY_id"]
        b['group'] = "B"

        
        c=c1.T
        c.columns = ["WY_id"]
        c['group'] = "C"

        lds1 = pd.concat([f,a,b,c], axis=0)
        lds2 = lds1.sort_values(by="WY_id")
        lds = lds2.dropna(subset=['WY_id']).reset_index(drop=True)

        self.model_groups_lastrow =lds
        return     self.model_groups_lastrow   
        

    def create_monthly (self):
        
        all_groups_count_monthly1 = self.all_groups_count.copy() 
        
        all_groups_count_monthly1['year']    = all_groups_count_monthly1.index.year
        all_groups_count_monthly1['month']   = all_groups_count_monthly1.index.month
        all_groups_count_monthly1['days']    = all_groups_count_monthly1.index.days_in_month
    
        self.all_groups_count_monthly   = all_groups_count_monthly1.groupby(['year', 'month', 'days']).agg(
            {'fresh' : 'mean',
             'groupA': 'mean',
             'groupB': 'mean',
             'groupC': 'mean',
             }
            ).reset_index()
        
        
        return self.all_groups_count_monthly
    
    def write_to_csv (self):
        
        reconcile_counts = pd.concat((self.all_groups_count, self.herd_daily), axis=1)

        reconcile_counts.to_csv('F:\\COWS\\data\\status\\reconcile_counts.csv')        
        self.fresh_ids  .to_csv('F:\\COWS\\data\\status\\fresh_ids_model.csv')
        self.group_A_ids.to_csv('F:\\COWS\\data\\status\\group_A_ids_model.csv')
        self.group_B_ids.to_csv('F:\\COWS\\data\\status\\group_B_ids_model.csv')
        self.group_C_ids.to_csv('F:\\COWS\\data\\status\\group_C_ids_model.csv')

        self.all_groups_count_monthly  .to_csv('F:\\COWS\\data\\status\\all_groups_count_monthly.csv')        

        # Replace NaN/NA in model_groups_dict before saving as JSON
        # cleaned_dict = self.replace_nan_in_dict(self.model_groups_dict)
        # with open("F:\\COWS\\data\\status\\model_groups_dict.json", 'w', encoding='utf-8') as f:
        #     json.dump(cleaned_dict, f, indent=2, default=str)           
    
if __name__ == "__main__":
    model_groups = ModelGroups()
    model_groups.load_and_process()    