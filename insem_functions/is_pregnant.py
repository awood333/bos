'''insem_functions\\is_pregnant.py'''
import inspect
import pandas as pd
from container import get_dependency

class IsPregnant:

    def __init__(self):

        print(f"IsPregnant instantiated by: {inspect.stack()[1].filename}")

        self.SD = None
        self.WD = None
        self.IUB = None
        self.IUD = None
        self.MB = None
        self.DR = None
        self.MA = None
        
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
        self.as_of_date = None
        
        #methods
        self.wet = None
        self.preg_df = None
        self.groups_count_daily = None



    def load(self):

        self.SD = get_dependency('status_data')
        self.WD = get_dependency('wet_dry')
        self.IUD= get_dependency('insem_ultra_data')
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')        
        self.MA = get_dependency('milk_aggregates')
        self.process()
        
    def process(self):
        self.startdate  = self.DR.startdate
        self.lastday    = self.MB.lastday
        
        #alive_ids includes heifers, milking and dry
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
        
   
        #methods
        self.wet_dry_letters, self.wd_lact_num = self.reform_period()
        self.ultra_4, self.ultra_pivot = self.create_ultra_ok_all_dates()
        self.create_preg_df()
  
        
        
    def reform_period(self):
        
        df = self.period
        regex_pattern = r'([A-Za-z]+)(\d+)'
        #([A-Za-z]+) captures one or more letters (the W, D, whatever prefix)
        #(\d+) captures one or more digits (the number)
        self.wet_dry_letters  = df.apply(lambda col: col.str.extract(regex_pattern)[0])
        self.wd_lact_num = df.apply(lambda col: col.str.extract(regex_pattern)[1]).astype(float)
        return self.wet_dry_letters, self.wd_lact_num
        

    def create_ultra_ok_all_dates(self):

        ultra_1 = self.MB.data['u'].loc[:,['wy_id','ultra_date','calf_num','readex']].copy()
        # ultra_1a= ultra_1.loc[(ultra_1['wy_id'])==94,:]
        ultra_2 = ultra_1.loc[(ultra_1['readex'] == 'ok')].reset_index(drop=True)
        ultra_3 = ultra_2[ultra_2['wy_id'].isin(self.alive_ids)].reset_index(drop=True)
        # #idxmax() returns the index label of the first occurrence of the maximum value for each group.
        # idx     = ultra_3.groupby(['wy_id', 'calf_num'])['ultra_date'].idxmax() 
        
        self.ultra_4 = (
            ultra_3.sort_values('ultra_date')
            .groupby(['wy_id', 'calf_num'],sort=False)
            .last()
            .reset_index()
            )
        
        ultra_5 = pd.pivot_table(self.ultra_4,
                                index = 'wy_id',
                                columns= 'calf_num',
                                values= 'ultra_date')
        self.ultra_pivot = ultra_5
        return self.ultra_4, self.ultra_pivot
    
    def create_preg_df(self):
        wyids = self.liters_T.index
        dates = self.liters_T.columns
        results = {}  # collect columns as series
        
        for i in wyids:
            preg1 = {}
            for date in dates:
                wd_lact_num = self.wd_lact_num.loc[i, date]
                
                if pd.isna(wd_lact_num):
                    preg1[date] = None
                else:
                    try:
                        start_date_date = self.start_lact.loc[i, wd_lact_num]
                    except KeyError:
                        print(f"Missing lact column for wy_id {i}: wd_lact_num = {wd_lact_num}")
                        preg1[date] = None
                        continue
                    try:
                        ultra_date = self.ultra_pivot.loc[i, wd_lact_num]
                        if pd.notna(ultra_date) and ultra_date < start_date_date:
                            preg1[date] = 'preg'
                        else:
                            preg1[date] = 'not_preg'
                    except KeyError:
                        preg1[date] = None
            
            results[i] = pd.Series(preg1)
        
        self.preg_df = pd.DataFrame(results)
        return self.preg_df
         
if __name__ == "__main__":
    model_groups = IsPregnant()
    model_groups.load()    