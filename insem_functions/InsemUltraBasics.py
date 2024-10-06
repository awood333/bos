'''InsemUltraBasics.py'''

import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime

tdy = pd.Timestamp.today()

class InsemUltraBasics:
    def __init__(self):
        

        self.df      = pd.DataFrame()
        
        self.data       = self.read_data()
        self.first_calf = self.create_first_calf()
        self.last_calf  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()
        self.IUB_dash_vars = self.get_dash_vars()
        
    def read_data(self):

        lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', index_col=None) 
        s   = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv')
        bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', index_col='WY_id')

        lb ['b_date']     = pd.to_datetime(lb['b_date'])
        s  ['stop']       = pd.to_datetime(s['stop'])        
        bd1['birth_date'] = pd.to_datetime(bd1['birth_date'])
        bd1['death_date'] = pd.to_datetime(bd1['death_date'])
        
        rng = list(range(1, bd1.index.max()+1))
        
        self.data = {
            'lb': lb,
            's': s,
            'bd1': bd1,
            'rng': rng
        }
        return self.data
    
    
    def create_first_calf(self):
        
        first_calf1 = self.data['lb'].groupby('WY_id').agg({
            'b_date'  : 'min',
            'calf#'   : 'min'
            }).reset_index()
        
        self.first_calf = first_calf1.set_index('WY_id').reindex(self.data['rng'])
        self.first_calf.rename(columns={'calf#': 'first calf#',
            'b_date': 'first calf bdate'}, inplace=True)     
        return self.first_calf

    

    def create_last_calf(self):
        
        last_calf1 = self.data['lb'].groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        
        self.last_calf = last_calf1.set_index("WY_id").reindex(self.data['rng'])
        self.last_calf.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'}, inplace=True)
        # self.last_calf['last calf#'].fillna( 0, inplace=True)
        
        self.last_calf.fillna({'last calf#': 0}, inplace=True)
        lastcalf_age = [(tdy - date).days for date in self.last_calf['last calf bdate']]
        self.last_calf['last calf age'] = lastcalf_age
        
        return self.last_calf
    


    def create_last_stop(self):
        last_stop1 = self.data['s'].groupby('WY_id').agg({
            'lact_num'    : 'max',
            'stop'        : 'max'
        })   
        
        self.last_stop = last_stop1.reindex(self.data['rng']).copy()
        self.last_stop.rename(columns = {'lact_num':'stop calf#','stop':'last stop date'
            },inplace=True)       
        
        # self.last_stop['last stop date'].fillna(np.nan, inplace=True)
        self.last_stop.fillna({'last stop date': np.nan}, inplace=True)
        
        return self.last_stop
    
    def get_dash_vars(self):
        self.IUB_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}

        return self.IUB_dash_vars

if __name__ == "__main__":
    iub=InsemUltraBasics()
    IUB_dash_vars = iub.get_dash_vars()
    
