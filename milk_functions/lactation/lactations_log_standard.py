'''Lactations'''
import inspect
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from container import get_dependency


class LactationsLogStandard:
    def __init__(self):
        print(f"LactationsLogStandard instantiated by: {inspect.stack()[1].filename}")
        self.MB  = None
        self.LB  = None
        self.L   = None
        self.TL = None #this_lactation
        self.LW  = None
        self.WD = None
            

        # Daily data
        self.m1_daily = None
        self.max_daily = None
        self.idx_max_daily = None
        self.max_daily_avg = None
        self.m1_maxdiff_daily = None
        self.m1_maxdiff_daily_avg = None
        self.daily_diff = None

        self.daily_log = None
        self.max_daily_log = None
        self.idx_max_daily_log = None
        self.maxdiff_daily_log = None
        self.zscore_daily_log = None
        self.avg_zscore_per_row_daily_log = None
        self.daily_diff_log  = None        

        # Weekly data
        self.m1_weekly = None
        self.idx_max_weekly = None
        self.max_weekly = None
        self.start_ratios = None
        self.start_peak_ratio = None
        self.start_peak_ratio_avg = None
        self.m1_maxdiff_weekly = None
        self.m1_maxdiff_weekly_avg = None
        self.weekly_diff = None 
        
        self.weekly_log = None
        self.max_weekly_log = None
        self.maxdiff_weekly_log = None
        self.zscore_weekly_log = None 
        self.avg_zscore_per_row_weekly_log = None
        self.weekly_diff_log = None

        # Standard deviations
        self.stddev_liters_daily = None
        self.stddev_liters_weekly = None


    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.LB = get_dependency('lactation_basics')
        self.L  = get_dependency('lactations')
        self.TL = get_dependency('this_lactation')
        self.LW = get_dependency('weekly_lactations')
        self.WD = get_dependency('wet_dry')

        self.get_days_milking()

        (self.m1_daily, 
                self.idx_max_daily, 
                self.max_daily, 
                self.m1_maxdiff_daily, 
                self.m1_maxdiff_daily_avg, 
                self.daily_diff
                )                   = self.create_daily()
    
        (self.daily_log,
            self.max_daily_log, 
            self.idx_max_daily_log,
            self.maxdiff_daily_log, 
            self.zscore_daily_log, 
            self.avg_zscore_per_row_daily_log, 
            self.daily_diff_log
            )                       = self.create_daily_log()
        
        (self.m1_weekly,
            self.max_weekly, 
            self.idx_max_weekly,
            self.start_peak_ratio,
            self.start_peak_ratio_avg,
            self.m1_maxdiff_weekly, 
            self.m1_maxdiff_weekly_avg, 
            self.weekly_diff    
                )                   = self.create_weekly()

        (self.weekly_log,
            self.max_weekly_log,   
            self.maxdiff_weekly_log, 
            self.zscore_weekly_log, 
            self.avg_zscore_per_row_weekly_log,
            self.weekly_diff_log
                )                  = self.create_weekly_log()
                

        self.stddev_liters_daily, self.stddev_liters_weekly = self.get_stddev_liters()
        self.plot_maxdiff()
        self.plot_liters()
        self.plot_zscore_comparison()

        self.write_to_csv() 


    def get_days_milking(self):
        date_of_change = pd.to_datetime('2025-09-27').date()
        # Ensure wdd index is also date type
        self.WD.wdd.index = self.WD.wdd.index.date
        # Now you can safely grab the row
        days_on_date_of_change = self.WD.wdd.loc[date_of_change]
        x=1
        
                    
    def create_daily(self):

        # Load daily lactation data
        m1_daily = self.TL.milking_daily.iloc[:365,:].copy()

        # Ensure columns and index are int for alignment
        m1_daily.columns = m1_daily.columns.astype(int)
        m1_daily.index   = m1_daily.index.astype(int)
        
        #for each cow
        max_daily           = m1_daily.max(axis=0).to_frame(name='max')   
        idx_max_daily       = m1_daily.idxmax(axis=0)
        max_daily.index     = max_daily.index.astype(int)
        m1_maxdiff_daily    = m1_daily - max_daily['max']
        daily_diff          = m1_daily.diff(periods=1)

        #for all cows
        max_daily_avg       = max_daily.mean(axis=0)    
        m1_maxdiff_daily_avg= m1_maxdiff_daily.mean(axis=1) # useful for finding peak


        # Assign to self for later use
        self.m1_daily   = m1_daily
        self.max_daily  = max_daily
        self.idx_max_daily          = idx_max_daily
        self.max_daily_avg          = max_daily_avg
        self.m1_maxdiff_daily       = m1_maxdiff_daily
        self.m1_maxdiff_daily_avg   = m1_maxdiff_daily_avg
        self.daily_diff             = daily_diff

        return (self.m1_daily, 
                self.idx_max_daily, 
                self.max_daily, 
                self.m1_maxdiff_daily, 
                self.m1_maxdiff_daily_avg, 
                self.daily_diff
                )

    def create_daily_log(self):

        daily_log           = np.log(self.m1_daily)
        max_daily_log       = daily_log.max(axis=0).to_frame(name='max')
        idx_max_daily_log   = daily_log.idxmax(axis=0)
        maxdiff_daily_log   = daily_log.divide(max_daily_log['max'], axis=1)
        daily_diff_log      = daily_log.diff(periods=1)

        # Z-score (stdev from mean) for each column (cow)
        zscore_daily_log     = maxdiff_daily_log.apply(lambda col: (col - col.mean()) / col.std(), axis=0)

        #  for all cows)
        avg_zscore_per_row_daily_log = zscore_daily_log.mean(axis=1)

        # assign to self
        self.daily_log       = daily_log
        self.max_daily_log      = max_daily_log
        self.idx_max_daily_log  = idx_max_daily_log
        self.maxdiff_daily_log  = maxdiff_daily_log
        self.zscore_daily_log   = zscore_daily_log
        self.daily_diff_log     = daily_diff_log

        #for all cows
        self.avg_zscore_per_row_daily_log = avg_zscore_per_row_daily_log
       

        return (self.daily_log,
                self.max_daily_log, 
                self.idx_max_daily_log,
                self.maxdiff_daily_log, 
                self.zscore_daily_log, 
                self.avg_zscore_per_row_daily_log, 
                self.daily_diff_log)

    def create_weekly(self):
        # Load weekly lactation data
        m1_weekly = self.TL.milking_wkly.iloc[:53,:].copy()

        # Ensure columns and index are int for alignment
        m1_weekly.columns    = m1_weekly.columns.astype(int)
        m1_weekly.index      = m1_weekly.index.astype(int)

        max_weekly           = m1_weekly.max(axis=0).to_frame(name='max')
        idx_max_weekly       = m1_weekly.idxmax(axis=0).dropna().astype(int)
        max_weekly.index     = max_weekly.index.astype(int)

        # Ratio of first 5 weeks to peak for each cow, as a DataFrame
        weeks = 20
        start_ratios = m1_weekly.iloc[:weeks].divide(max_weekly['max'], axis=1)
        start_ratios.index = [f"week_{i+1}" for i in range(weeks)]

        # Vectorized: get the first value for each column, and the max value using idx_max_weekly (converted to int)
        first_week = m1_weekly.iloc[0].to_frame(name='first week liters')
        start_peak_ratio = first_week['first week liters'] / max_weekly['max']

        # Average for all cows
        start_peak_ratio_avg = start_peak_ratio.mean(axis=0)
        
        m1_maxdiff_weekly    = m1_weekly - max_weekly['max']    #each cow
        weekly_diff          = m1_weekly.diff(periods=1)

        # all cows
        m1_maxdiff_weekly_avg= m1_maxdiff_weekly.mean(axis=1) # all cows       useful for finding peak
      

        # Assign to self for later use
        self.m1_weekly              = m1_weekly
        self.max_weekly             = max_weekly
        self.idx_max_weekly         = idx_max_weekly
        self.start_ratios           = start_ratios
        self.start_peak_ratio       = start_peak_ratio
        self.start_peak_ratio_avg   = start_peak_ratio_avg
        self.m1_maxdiff_weekly      = m1_maxdiff_weekly
        self.m1_maxdiff_weekly_avg  = m1_maxdiff_weekly_avg
        self.weekly_diff            = weekly_diff

        return (self.m1_weekly,
                self.max_weekly,                
                self.idx_max_weekly,
                self.start_peak_ratio,
                self.start_peak_ratio_avg, 
                self.m1_maxdiff_weekly, 
                self.m1_maxdiff_weekly_avg, 
                self.weekly_diff    
                )


    def create_weekly_log(self):
        # Log transform weekly data
        weekly_log = np.log(self.m1_weekly)
        max_weekly_log = weekly_log.max(axis=0).to_frame(name='max')

        maxdiff_weekly_log = weekly_log.divide(max_weekly_log['max'], axis=1)   #log - so 'divide not subtract

        zscore_weekly_log  = maxdiff_weekly_log.apply(lambda col: (col - col.mean()) / col.std(), axis=0)
        avg_zscore_per_row_weekly_log  = zscore_weekly_log.mean(axis=1)  #all cows
        weekly_diff_log                = maxdiff_weekly_log.diff(periods=1)

        self.weekly_log = weekly_log
        self.max_weekly_log = max_weekly_log
        self.maxdiff_weekly_log = maxdiff_weekly_log
        self.zscore_weekly_log = zscore_weekly_log
        self.avg_zscore_per_row_weekly_log = avg_zscore_per_row_weekly_log
        self.weekly_diff_log = weekly_diff_log

        return (self.weekly_log,
            self.max_weekly_log,   
            self.maxdiff_weekly_log, 
            self.zscore_weekly_log, 
            self.avg_zscore_per_row_weekly_log,
            self.weekly_diff_log
                )

    def get_stddev_liters(self):
        self.m1_weekly = self.TL.milking_wkly
        self.m1_daily = self.TL.milking_daily
        

        # Log difference for weekly and daily
        wkly_log = np.log(self.m1_weekly)
        wkly_logdiff = wkly_log.diff().dropna(how='all')
        stddev_liters_weekly1 = wkly_logdiff.std(axis=0).to_frame(name='stdev liters')
        stddev_liters_weekly2 = np.exp(stddev_liters_weekly1)

        daily_log = np.log(self.m1_daily)
        daily_logdiff = daily_log.diff().dropna(how='all')
        stddev_liters_daily1 = daily_logdiff.std(axis=0).to_frame(name='stdev liters')
        stddev_liters_daily2 = np.exp(stddev_liters_daily1)


        #merge with the max value of each lact
        self.stddev_liters_weekly  = pd.merge(stddev_liters_weekly2,  self.max_weekly,  left_index=True, right_index=True)
        self.stddev_liters_daily = pd.merge(stddev_liters_daily2, self.max_daily, left_index=True, right_index=True)


        return self.stddev_liters_weekly, self.stddev_liters_daily
            
    def plot_liters(self):
        fig, ax1 = plt.subplots(figsize=(14, 7))

        # Weekly mean liters (raw) - left y-axis, bottom x-axis
        weekly_means = self.m1_weekly.mean(axis=1)
        ax1.plot(np.arange(1, 53), weekly_means.head(52).values, 'o-', color='blue', label='Weekly Mean Liters (Raw)')
        ax1.set_xlabel('Week')
        ax1.set_ylabel('Mean Liters (Raw)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)
        ax1.grid(axis='x', linestyle=':', alpha=0.7)
        ax1.set_xlim(1, 52)

        # Daily mean liters (raw) - left y-axis, top x-axis, dotted, light, no markers
        ax2 = ax1.twiny()
        daily_means = self.m1_daily.mean(axis=1)
        ax2.plot(np.arange(1, 365), daily_means.head(364).values, linestyle=':', color='orange', label='Daily Mean Liters (Raw)', alpha=0.7)
        ax2.set_xlabel('Day')
        ax2.set_xlim(1, 364)
        ax2.grid(axis='x', linestyle=':', alpha=0.7)

        # Legends (combine all)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.title('Weekly & Daily Mean Liters (Raw) with Dual X Axes')
        plt.tight_layout()
        plt.savefig("F:\\COWS\\data\\milk_data\\lactations\\plots\\mean_liters.png")
        print("mean_liters.png updated")
        # plt.show()



    def plot_maxdiff(self):
        fig, ax1 = plt.subplots(figsize=(14, 7))

        # Weekly avg maxdiff - left y-axis, bottom x-axis
        weekly = self.m1_maxdiff_weekly_avg
        if weekly is not None:
            ax1.plot(np.arange(1, 53), weekly.head(52).values, 'o-', color='blue', label='Weekly Avg MaxDiff')
        ax1.set_xlabel('Week')
        ax1.set_ylabel('Avg MaxDiff (from max)')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)
        ax1.grid(axis='x', linestyle=':', alpha=0.7)
        ax1.set_xlim(1, 52)

        # Daily avg maxdiff - left y-axis, top x-axis (shared y, different x)
        ax2 = ax1.twiny()
        daily = self.m1_maxdiff_daily_avg
        if daily is not None:
            ax2.plot(np.arange(1, 365), 
                     daily.head(364).values, 
                     linestyle='dotted',
                     linewidth=2.5, 
                     color='orange', 
                     label='Daily Avg MaxDiff', 
                     alpha=0.7)
            
        ax2.set_xlabel('Day')
        ax2.set_xlim(1, 364)
        ax2.grid(axis='x', linestyle=':', alpha=0.7)

        # Legends (combine all)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

        plt.title('Weekly / Daily MaxDiff avg')
        plt.tight_layout()
        plt.savefig("F:\\COWS\\data\\milk_data\\lactations\\plots\\maxdiff_avg.png")        
        # plt.show()


    def plot_zscore_comparison(self):

        fig, ax1 = plt.subplots(figsize=(12, 6))

        # Plot weekly on primary x-axis
        ax1.plot(self.avg_zscore_per_row_weekly_log.index, self.avg_zscore_per_row_weekly_log.values, 'o-', color='blue', label='Weekly Avg Z-Score')
        ax1.set_xlabel('Week')
        ax1.set_ylabel('Weekly Avg Z-Score', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        # Add horizontal gridlines
        ax1.grid(axis='y', linestyle='--', alpha=0.7)

        # Create secondary x-axis for daily
        ax2 = ax1.twiny()
        ax2.plot(self.avg_zscore_per_row_daily_log.index, self.avg_zscore_per_row_daily_log.values, '--', color='red', label='Daily Avg Z-Score')
        ax2.set_xlabel('Day')
        ax2.set_ylabel('Daily Avg Z-Score', color='red')
        ax2.tick_params(axis='y', labelcolor='red')

        # Add legends
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        plt.title('Comparison of Weekly and Daily Avg Z-Score')
        plt.tight_layout()
        plt.savefig("F:\\COWS\\data\\milk_data\\lactations\\plots\\zscore_comparison.png")
        # plt.show()        
 



    def write_to_csv(self):
        
        self.stddev_liters_daily    .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\StDev_liters_per_cow_daily.csv")
        self.m1_maxdiff_daily_avg   .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\m1_maxdiff_daily_avg.csv")
        self.m1_maxdiff_daily       .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\m1_maxdiff_daily.csv")
        self.max_daily              .to_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\max_daily.csv")

        self.m1_weekly              .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\m1_weekly.csv")
        self.start_peak_ratio       .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\start_peak_ratio.csv")
        self.idx_max_weekly         .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\idx_max_weekly.csv")
        self.stddev_liters_weekly   .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\StDev_liters_per_cow_wkly.csv")
        self.m1_maxdiff_weekly_avg  .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\m1_maxdiff_weekly_avg.csv")
        self.max_weekly             .to_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\max_weekly.csv")


if __name__ == "__main__":
    self = LactationsLogStandard()
    self.load_and_process()    
