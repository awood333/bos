'''milk_functions\\model_groups.py'''
import inspect
import pandas as pd
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
        self.DRM= None
        self.MA = None
        self.IP = None
        self.BSO = None
        
        #process
        self.startdate = None
        self.lastday  = None
        self.fullday = None
        self.wet_dry_days_weekly = None
        self.wet_period_weekly = None
        self.alive_ids = None
        self.ultra_4 = None
        self.ultra_pivot = None
        self.wet_dry_letters = None
        self.wd_lact_num = None
        self.daynums = None
        self.liters_T = None
        self.period = None
        self.start_lact = None
        self.stop_lact = None
        self.pregnant = None
        self.group_df = None



    def load(self):

        self.SD = get_dependency('status_data')
        self.WD = get_dependency('wet_dry')
        self.IUD= get_dependency('insem_ultra_data')
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')        
        self.MA = get_dependency('milk_aggregates')
        self.IP = get_dependency('is_pregnant')
        self.process()
        
        
    def process(self):
        
        self.DRM = self.DR.date_range_weekly
        self.startdate = self.DR.startdate
        self.lastday  = self.MB.lastday

        self.alive_ids  = self.SD.alive_ids_today
        self.fullday    = self.MA.weekly_average_date[self.MA.weekly_average_date.index >= pd.to_datetime(self.startdate)]
        
        self.wet_dry_days_weekly  = self.WD.wet_dry_days_weekly[
            self.WD.wet_dry_days_weekly.index >= pd.to_datetime(self.startdate)]\
            .reset_index().rename(columns={'index': 'date'}).set_index('date')
            
        self.wet_period_weekly = self.WD.wet_dry_period_weekly[
            self.WD.wet_dry_period_weekly.index  >= pd.to_datetime(self.startdate)]\
            .reset_index().rename(columns={'index': 'date'}).set_index('date')
            
        self.daynums = self.wet_dry_days_weekly[self.alive_ids].T
        self.liters_T  = self.fullday[self.alive_ids].T
        self.period  = self.wet_period_weekly[self.alive_ids].T
        
        start_lact_1 = self.MB.data['start_pivot']
        self.start_lact = start_lact_1.loc[self.alive_ids, :] #cols are lact nums, rows are wy
        
        stop_lact_1  = self.MB.data['stop_pivot']
        self.stop_lact  = stop_lact_1.loc[self.alive_ids, :]  
        
        self.pregnant = self.IP.preg_df.T
        
              
        #methods
        self.group_df = self.create_model_groups()
       

        

    def create_model_groups (self):

        liters = self.liters_T
        wyids = liters.index
        dates = liters.columns
        wetdry = self.wet_dry_days_weekly
        day_num = self.daynums
        period = self.period
        pregnant   = self.pregnant
        
        group_df = pd.DataFrame(index=dates)

        for wy in wyids:
            
            group_1 = {}  # reset for each wy_id
            for date in dates:
                
                liters_1    = liters .at[wy, date]
                day_num_1   = day_num.at[wy, date]
                preg_1      = pregnant.at[wy, date]


                if pd.isna(day_num_1) or pd.isna(liters_1):
                    group_1[date] = None
                if day_num_1 < 21:
                    group_1[date] ='F'
                elif day_num_1 >= 21 and liters_1 >= 15:
                    group_1[date] ='A'
                elif day_num_1 >= 21 and 0 < liters_1 < 15:
                    if preg_1 == 'pregnant':
                        group_1[date] = 'C'
                    else:
                        group_1[date] =  'B'
                elif liters_1 == 0:
                    group_1[date] = 'D'


            group_df[wy] = pd.Series(group_1)
        
        self.group_df = group_df
        return self.group_df



         
if __name__ == "__main__":
    model_groups = ModelGroups()
    model_groups.load()    
    
    
    