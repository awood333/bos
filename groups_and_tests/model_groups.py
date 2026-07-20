'''milk_functions\\model_groups.py'''
import inspect
import pandas as pd
import numpy as np
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
        self.weeknums = None
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
        self.fullday    = self.MA.weekly_average_date[self.MA.weekly_average_date.
                        index >= pd.to_datetime(self.startdate)]
        
        self.wet_dry_days_weekly  = self.WD.wet_dry_days_weekly[
            self.WD.wet_dry_days_weekly.index >= pd.to_datetime(self.startdate)]\
            .reset_index().rename(columns={'index': 'date'}).set_index('date')
            
        self.wet_period_weekly = self.WD.period_weekly[
            self.WD.period_weekly.index  >= pd.to_datetime(self.startdate)]\
            .reset_index().rename(columns={'index': 'date'}).set_index('date')
            
        self.weeknums = self.wet_dry_days_weekly[self.alive_ids].T
        self.liters_T  = self.fullday[self.alive_ids].T
        self.period  = self.wet_period_weekly[self.alive_ids].T
        
        start_lact_1 = self.MB.data['start_pivot']
        self.start_lact = start_lact_1.loc[self.alive_ids, :] #cols are lact nums, rows are wy
        
        stop_lact_1  = self.MB.data['stop_pivot']
        self.stop_lact  = stop_lact_1.loc[self.alive_ids, :]  
        
        self.pregnant = self.IP.preg_df.T
        
              
        #methods
        self.group_df = self.create_model_groups()
       

    
    def create_model_groups(self):
        liters   = self.liters_T
        week_num  = self.weeknums
        pregnant = self.pregnant

        # explicit alignment guard: if these three ever drift out of sync
        # (different wy_id/date coverage), this catches it instead of
        # silently misaligning cells the way three independent .at[] calls could

        liters_a, week_num_a = liters.align(week_num, join='inner')
        liters_a, pregnant_a = liters_a.align(pregnant, join='inner')
        week_num_a = week_num_a.reindex_like(liters_a)
        pregnant_a = pregnant_a.reindex_like(liters_a)
        

        

        missing = week_num_a.isna() | liters_a.isna()
        is_preg = pregnant_a == 'preg'
        
        # check alignment
        # print('liters_a: '  ,liters_a .iloc[94,-1])
        # print('week_num_a: ' ,week_num_a.iloc[94,-1])
        print('94 is_preg - pregnant   :' ,pregnant.loc[94].iloc[-5:-1])
        print('94 is_preg - pregnant_a :' ,pregnant_a.loc[94,] .iloc[-5:-1])
        
        conditions = [
            missing, # highest priority. np.select locks in None for any missing-data cell and never 
                        #evaluates the later conditions for that cell
            week_num_a < 21,
            (week_num_a >= 21) & (liters_a >= 15),
            (week_num_a >= 21) & (liters_a > 0) & (liters_a < 15) & is_preg,
            (week_num_a >= 21) & (liters_a > 0) & (liters_a < 15) & ~is_preg,
            liters_a == 0,
        ]
        choices = [None, 'F', 'A', 'C', 'B', 'D']

        group_arr = np.select(conditions, choices, default=None) 
            #group_arr is computed as one full 2D array in a single np.select call, 
            # #and pd.DataFrame(group_arr, ...) constructs the whole frame in one allocation
        group_df = pd.DataFrame(group_arr, index=liters_a.index, columns=liters_a.columns).T
        # .T at the end: index=wy_id/columns=date -> index=date/columns=wy_id, matching original shape

        self.group_df = group_df
        return self.group_df



         
if __name__ == "__main__":
    model_groups = ModelGroups()
    model_groups.load()
    