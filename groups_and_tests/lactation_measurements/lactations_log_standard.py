'''milk_functions/lactation/lactations_log_standard.py'''
import inspect
import matplotlib.pyplot as plt
from container import get_dependency

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')


class LactationsLogStandard:
    def __init__(self):
        print(f"LactationsLogStandard instantiated by: {inspect.stack()[1].filename}")
        self.MB  = None
        self.LB  = None
        self.L   = None
        self.TL = None #this_lactation
        self.LW  = None
        self.WD = None
            
        self.input_WY_id =None
        self.WY_id = None
        self.days_on_date_of_change = None
        self.date_of_change = None
        self.lastday = None

        # Daily data
        self.m1_daily_all = None
        self.max_daily_all = None
        self.idx_max_daily_all = None

        self.max_daily_avg_all = None
        self.maxdiff_daily_all = None
        
        self.m1_maxdiff_daily_avg_all = None
        self.daily_diff_all = None

        self.m1_daily_cow = None
        self.max_daily_cow = None
        self.idx_max_daily_cow = None
        self.max_daily_avg_cow = None
        self.maxdiff_daily_cow = None
        self.maxdiff_daily_avg_cow = None
        self.daily_cow_diffy_avg_cow = None
        self.daily_cow_diff = None

        # Weekly data
        self.m1_weekly_all = None
        self.max_weekly_all = None
        self.idx_max_weekly_all = None

        self.max_weekly_avg_all = None
        self.maxdiff_weekly_avg_all = None
        self.maxdiff_weekly_all = None
        self.weekly_diff_all = None

        self.start_ratios_all = None
        self.start_peak_ratio_all = None
        self.start_peak_ratio_avg_all = None
        
        self.m1_weekly_cow = None
        self.max_weekly_cow = None
        self.idx_max_weekly_cow = None
        self.max_weekly_avg_cow = None
        self.maxdiff_weekly_cow = None
        self.maxdiff_weekly_avg_cow = None
        self.weekly_diff_cow = None

        # Standard deviations
        self.stddev_liters_daily_all = None
        self.stddev_liters_weekly_all = None


    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.LB = get_dependency('lactation_basics')
        self.L  = get_dependency('lactations')
        self.TL = get_dependency('this_lactation')
        self.LW = get_dependency('weekly_lactations')
        self.WD = get_dependency('wet_dry')

        self.date_of_change = pd.to_datetime('2025-09-27').date()
        self.lastday = self.MB.lastday #timestamp
        
        # Methods

        self.input_WY_id            = self.create_input_WY_id()
        self.days_on_date_of_change = self.get_days_milking()


        (self.m1_daily_all, 
            self.max_daily_all, 
            self.idx_max_daily_all, 
    
            self.max_daily_avg_all,
            self.maxdiff_daily_all, 
            
            self.m1_maxdiff_daily_avg_all, 
            self.daily_diff_all
            )                   = self.create_daily_all()

        if self.WY_id is None and self.m1_daily_all is not None:
            self.WY_id = self.m1_daily_all.columns[0]

        (   self.m1_daily_cow,
            self.max_daily_cow,
            self.idx_max_daily_cow,
            self.max_daily_avg_cow,
            self.maxdiff_daily_cow,
            self.maxdiff_daily_avg_cow,
            self.daily_cow_diff
        )                           = self.create_daily_cow()

        (   self.m1_weekly_all,
            self.max_weekly_all,
            self.idx_max_weekly_all,

            self.max_weekly_avg_all,  #
            self.maxdiff_weekly_all,
            self.maxdiff_weekly_avg_all,

            self.weekly_diff_all,
            self.start_ratios_all,
            self.start_peak_ratio_all,
            self.start_peak_ratio_avg_all
        )                = self.create_weekly_all()

        (   
            self.m1_weekly_cow,
            self.max_weekly_cow,
            self.idx_max_weekly_cow,
            self.max_weekly_avg_cow,
            self.maxdiff_weekly_cow,
            self.maxdiff_weekly_avg_cow,
            self.weekly_diff_cow
        )               = self.create_weekly_cow()

                

        self.stddev_liters_daily_all, self.stddev_liters_weekly_all = self.get_stddev_liters()


        self.write_to_csv() 

    def create_input_WY_id(self):
        return self.WY_id


    def get_days_milking(self):
        # Ensure wdd index is a DatetimeIndex
        if not isinstance(self.WD.wet_days_df.index, pd.DatetimeIndex):
            self.WD.wet_days_df.index = pd.to_datetime(self.WD.wet_days_df.index)
        # Now you can safely use .date
        self.WD.wet_days_df.index = self.WD.wet_days_df.index.date
        # Now you can safely grab the row
        self.days_on_date_of_change = pd.DataFrame(self.WD.wet_days_df.loc[self.date_of_change])
        return self.days_on_date_of_change
                 
    def create_daily_all(self):


        # Load daily data from ThisLactation -- index is from day 1 (not date)
        m1_daily_all = self.TL.milking_daily.iloc[:365,:].copy().dropna(axis=1, how='all')

        # Ensure columns and index are int for alignment
        m1_daily_all.columns = m1_daily_all.columns.astype(int)
        m1_daily_all.index   = m1_daily_all.index.astype(int)

        
        # creates a single df for all cows
        max_daily_all           = m1_daily_all.max(axis=0).to_frame(name='max')   
        idx_max_daily_all       = m1_daily_all.idxmax(axis=0, skipna=True)
        max_daily_all.index     = max_daily_all.index.astype(int)
        maxdiff_daily_all       = m1_daily_all - max_daily_all['max']
        daily_diff_all          = m1_daily_all.diff(periods=1)

        #for all cows
        max_daily_avg_all       = max_daily_all.mean(axis=0)    
        m1_maxdiff_daily_avg_all= maxdiff_daily_all.mean(axis=1) # useful for finding peak


        # Assign to self for later use
        self.m1_daily_all           = m1_daily_all
        self.max_daily_all          = max_daily_all
        self.idx_max_daily_all      = idx_max_daily_all

        self.max_daily_avg_all      = max_daily_avg_all
        self.maxdiff_daily_all      = maxdiff_daily_all

        self.m1_maxdiff_daily_avg_all   = m1_maxdiff_daily_avg_all
        self.daily_diff_all             = daily_diff_all

        return (self.m1_daily_all, 
                self.max_daily_all, 
                self.idx_max_daily_all, 
        
                self.max_daily_avg_all,
                self.maxdiff_daily_all, 

                self.m1_maxdiff_daily_avg_all, 
                self.daily_diff_all
                )

    
    def create_daily_cow(self):
        # Load daily data from ThisLactation -- index is from day 1 (not date)
        m1_daily_cow = self.m1_daily_all.loc[:, self.WY_id].copy()

        # Ensure columns and index are int for alignment
        m1_daily_cow = m1_daily_cow.astype(float)
        m1_daily_cow.index = m1_daily_cow.index.astype(int)

        # For the cow
        max_daily_cow       = self.max_daily_all.loc[self.WY_id]
        idx_max_daily_cow   = self.idx_max_daily_all.loc[self.WY_id]
        maxdiff_daily_cow   = m1_daily_cow - max_daily_cow['max']
        daily_cow_diff      = m1_daily_cow.diff(periods=1)


        # For the cow (single value)
        max_daily_avg_cow = max_daily_cow.mean(axis=0)
        maxdiff_daily_avg_cow = maxdiff_daily_cow.mean(axis=0)  # useful for finding peak

        # Assign to self for later use
        self.m1_daily_cow = m1_daily_cow
        self.max_daily_cow = max_daily_cow
        self.idx_max_daily_cow = idx_max_daily_cow
        self.max_daily_avg_cow = max_daily_avg_cow
        self.maxdiff_daily_cow = maxdiff_daily_cow
        self.maxdiff_daily_avg_cow = maxdiff_daily_avg_cow
        self.daily_cow_diff = daily_cow_diff

        return (
            self.m1_daily_cow,
            self.max_daily_cow,
            self.idx_max_daily_cow,
            self.max_daily_avg_cow,
            self.maxdiff_daily_cow,
            self.maxdiff_daily_avg_cow,
            self.daily_cow_diff
        )


    def create_weekly_all(self):
        # Load weekly lactation data from ThisLactation -- index is from week 1 (not date)
        m1_weekly_all = self.TL.milking_wkly.iloc[:53, :].copy().dropna(axis=1, how='all')

        # Ensure columns and index are int for alignment
        m1_weekly_all.columns = m1_weekly_all.columns.astype(int)
        m1_weekly_all.index = m1_weekly_all.index.astype(int)

        # creates a single df for all cows
        max_weekly_all = m1_weekly_all.max(axis=0).to_frame(name='max')
        idx_max_weekly_all = m1_weekly_all.idxmax(axis=0, skipna=True)
        max_weekly_all.index = max_weekly_all.index.astype(int)
        maxdiff_weekly_all = m1_weekly_all - max_weekly_all['max']
        weekly_diff_all = m1_weekly_all.diff(periods=1)

        # for all cows
        max_weekly_avg_all = max_weekly_all.mean(axis=0)
        maxdiff_weekly_avg_all = maxdiff_weekly_all.mean(axis=1)  # useful for finding peak

        # Ratio of first 20 weeks to peak for each cow, as a DataFrame
        weeks = 20
        start_ratios_all = m1_weekly_all.iloc[:weeks].divide(max_weekly_all['max'], axis=1)
        start_ratios_all.index = [f"week_{i+1}" for i in range(weeks)]

        # Vectorized: get the first value for each column, and the max value using idx_max_weekly_all (converted to int)
        first_week_all = m1_weekly_all.iloc[0].to_frame(name='first week liters')
        start_peak_ratio_all = first_week_all['first week liters'] / max_weekly_all['max']
        start_peak_ratio_avg_all = start_peak_ratio_all.mean(axis=0)

        # Assign to self for later use
        self.m1_weekly_all = m1_weekly_all
        self.max_weekly_all = max_weekly_all
        self.idx_max_weekly_all = idx_max_weekly_all

        self.max_weekly_avg_all = max_weekly_avg_all
        self.maxdiff_weekly_all = maxdiff_weekly_all
        self.maxdiff_weekly_avg_all = maxdiff_weekly_avg_all

        self.weekly_diff_all = weekly_diff_all
        self.start_ratios_all = start_ratios_all
        self.start_peak_ratio_all = start_peak_ratio_all
        self.start_peak_ratio_avg_all = start_peak_ratio_avg_all

        return (
            self.m1_weekly_all,
            self.max_weekly_all,
            self.idx_max_weekly_all,

            self.max_weekly_avg_all,  #
            self.maxdiff_weekly_all,
            self.maxdiff_weekly_avg_all,

            self.weekly_diff_all,
            self.start_ratios_all,
            self.start_peak_ratio_all,
            self.start_peak_ratio_avg_all
        )


    def create_weekly_cow(self):
        # Load weekly data for the cow
        m1_weekly_cow = self.m1_weekly_all.loc[:, self.WY_id].copy()

        # Ensure columns and index are int for alignment
        m1_weekly_cow = m1_weekly_cow.astype(float)
        m1_weekly_cow.index = m1_weekly_cow.index.astype(int)

        # For the cow
        max_weekly_cow = self.max_weekly_all.loc[self.WY_id]
        idx_max_weekly_cow = self.idx_max_weekly_all.loc[self.WY_id]
        maxdiff_weekly_cow = m1_weekly_cow - max_weekly_cow['max']
        weekly_diff_cow = m1_weekly_cow.diff(periods=1)

        # For the cow (single value)
        max_weekly_avg_cow = max_weekly_cow.mean(axis=0)
        maxdiff_weekly_avg_cow = maxdiff_weekly_cow.mean(axis=0)  # useful for finding peak

        # Assign to self for later use
        self.m1_weekly_cow      = m1_weekly_cow
        self.max_weekly_cow     = max_weekly_cow
        self.idx_max_weekly_cow = idx_max_weekly_cow
        self.max_weekly_avg_cow = max_weekly_avg_cow
        self.maxdiff_weekly_cow = maxdiff_weekly_cow
        self.maxdiff_weekly_avg_cow = maxdiff_weekly_avg_cow
        self.weekly_diff_cow    = weekly_diff_cow

        return (
            self.m1_weekly_cow,
            self.max_weekly_cow,
            self.idx_max_weekly_cow,
            self.max_weekly_avg_cow,
            self.maxdiff_weekly_cow,
            self.maxdiff_weekly_avg_cow,
            self.weekly_diff_cow
        )




    def get_stddev_liters(self):
        self.m1_weekly = self.TL.milking_wkly
        self.m1_daily_all = self.TL.milking_daily
        

        # Log difference for weekly and daily
        wkly_log = np.log(self.m1_weekly_all)
        wkly_logdiff = wkly_log.diff().dropna(how='all')
        stddev_liters_weekly1 = wkly_logdiff.std(axis=0).to_frame(name='stdev liters')
        stddev_liters_weekly2 = np.exp(stddev_liters_weekly1)

        daily_log = np.log(self.m1_daily_all)
        daily_logdiff = daily_log.diff().dropna(how='all')
        stddev_liters_daily1 = daily_logdiff.std(axis=0).to_frame(name='stdev liters')
        stddev_liters_daily2 = np.exp(stddev_liters_daily1)


        #merge with the max value of each lact
        self.stddev_liters_weekly_all  = pd.merge(stddev_liters_weekly2,  self.max_weekly_all,  left_index=True, right_index=True)
        self.stddev_liters_daily_all = pd.merge(stddev_liters_daily2, self.max_daily_all, left_index=True, right_index=True)


        return self.stddev_liters_weekly_all, self.stddev_liters_daily_all
            
    def write_to_csv(self):
        
        self.stddev_liters_daily_all    .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\StDev_liters_per_cow_daily_all.csv")
        self.m1_maxdiff_daily_avg_all   .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\m1_maxdiff_daily_avg_all.csv")
        self.maxdiff_daily_all      .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\maxdiff_daily_all.csv")
        self.max_daily_all          .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\max_daily_all.csv")

        self.m1_weekly                  .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\m1_weekly.csv")
        self.start_peak_ratio_all       .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\start_peak_ratio_all.csv")
        self.idx_max_weekly_all         .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\idx_max_weekly_all.csv")
        self.stddev_liters_weekly_all   .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\StDev_liters_per_cow_wkly_all.csv")
        self.maxdiff_weekly_avg_all  .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\maxdiff_weekly_avg_all.csv")
        self.max_weekly_all             .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\max_weekly_all.csv")


if __name__ == "__main__":
    obj = LactationsLogStandard()
    obj.load_and_process()    
