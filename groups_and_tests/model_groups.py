'''milk_functions\\model_groups.py'''
import inspect
import pandas as pd
# import json
# import math

from container import get_dependency


class ModelGroups:

    def __init__(self):

        print(f"ModelGroups instantiated by: {inspect.stack()[1].filename}")

        self.SD = None
        self.WD = None
        self.IUB = None
        self.IUD = None
        self.MB = None
        self.DR = None
        self.DRM = None
        self.startdate = None
        self.lastday  = None
        self.WD_weekly = None
        self.wet = None
        self.groups_count_daily = None



    def load_and_process(self):

        self.SD = get_dependency('status_data')
        self.WD = get_dependency('wet_dry')
        self.IUB= get_dependency('insem_ultra_basics')
        self.IUD= get_dependency('insem_ultra_data')
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        
        self.DRM = self.DR.date_range_monthly_data
        self.startdate = self.DR.startdate
        self.lastday  = self.MB.lastday

        self.WD_weekly = self.WD.wet_dry_weekly[self.WD.wet_dry_weekly['date'] >= pd.to_datetime(self.startdate)].reset_index(drop=True)
        
        #methods
        self.wet      = self.create_model_groups()
        self.groups_count_daily = self.group_counts_by_date()
        self.write_to_csv()

        

    def create_model_groups (self):

        wet = self.WD_weekly

        preg = self.IUD.all_preg.reset_index(drop=True)
        pregnant_mask1 = preg[['WY_id', 'status']]
        pregnant_mask2 = pregnant_mask1.loc[pregnant_mask1['status'] == 'M'].reset_index(drop=True)
        pregnant_mask = pd.Series(pregnant_mask2['WY_id'])

        def assign_group(row):
            day_num = row['day_num']
            liters  = row['liters']
            wy_id   = row['WY_id']
            if pd.isna(day_num) or pd.isna(liters):
                return None
            if day_num < 21:
                return 'F'
            elif day_num >= 21 and liters >= 15:
                return 'A'
            elif day_num >= 21 and 0 < liters < 15:
                if wy_id in pregnant_mask.values:
                    return 'C'
                else:
                    return 'B'
            elif liters == 0:
                return 'D'
            else:
                return None

        wet['group'] = wet.apply(assign_group, axis=1)
        self.wet = wet    

        return self.wet

    def group_counts_by_date(self):
        # Create date range index
        date_index = pd.date_range(start=self.startdate, end=self.lastday, freq='D')
        # Initialize blank DataFrame
        result = pd.DataFrame(0, index=date_index, columns=['F', 'A', 'B', 'C', 'D'])
        # Count group occurrences per date
        for _, row in self.wet.iterrows():
            date = pd.to_datetime(row['date'])
            group = row['group']
            if group in result.columns and date in result.index:
                result.at[date, group] += 1
        # Add totals column
        result['totals'] = result.sum(axis=1)
        self.groups_count_daily = result
        return self.groups_count_daily
    
    def write_to_csv (self):
        self.wet     .to_csv("F:\\COWS\\data\\groups_and_tests\\wet.csv")
        self.groups_count_daily.to_csv("F:\\COWS\\data\\groups_and_tests\\groups_count_daily.csv")
    
if __name__ == "__main__":
    model_groups = ModelGroups()
    model_groups.load_and_process()    