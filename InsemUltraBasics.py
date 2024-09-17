'''InsemUltraBasics.py'''

import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
class InsemUltraBasics:
    def __init__(self):
        
        self.tdy = pd.Timestamp.today()
        self.lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates= ['b_date'], index_col=None) 
        self.s   = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv', parse_dates= ['stop'])
        self.bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], index_col='WY_id')

        
        self.rng     = list(range(1, self.bd1.index.max()+1))
        self.df      = pd.DataFrame()
        self.first_calf = self.create_first_calf()
        self.last_calf  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()

    def create_first_calf(self):
        
        first_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'min',
            'calf#'   : 'min'
            }).reset_index()
        
        self.first_calf = first_calf1.set_index('WY_id').reindex(self.rng)
        self.first_calf.rename(columns={'calf#': 'first calf#',
            'b_date': 'first calf bdate'}, inplace=True)
        
        return self.first_calf
    

    def create_last_calf(self):
        
        last_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        
        self.last_calf = last_calf1.set_index("WY_id").reindex(self.rng)
        self.last_calf.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'}, inplace=True)
        # self.last_calf['last calf#'].fillna( 0, inplace=True)
        
        self.last_calf.fillna({'last calf#': 0}, inplace=True)
        lastcalf_age = [(self.tdy - date).days for date in self.last_calf['last calf bdate']]
        self.last_calf['last calf age'] = lastcalf_age
        
        return self.last_calf
    


    def create_last_stop(self):
        last_stop1 = self.s.groupby('WY_id').agg({
            'lact_num'    : 'max',
            'stop'        : 'max'
        })   
        
        self.last_stop = last_stop1.reindex(self.rng).copy()
        self.last_stop.rename(columns = {'lact_num':'stop calf#','stop':'last stop date'
            },inplace=True)       
        
        # self.last_stop['last stop date'].fillna(np.nan, inplace=True)
        self.last_stop.fillna({'last stop date': np.nan}, inplace=True)
        
        return self.last_stop

