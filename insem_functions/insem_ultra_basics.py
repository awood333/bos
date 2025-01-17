'''InsemUltraBasics.py'''

import pandas as pd
import numpy as np
# from datetime import date, timedelta, datetime

from MilkBasics import MilkBasics

tdy = pd.Timestamp.today()

class InsemUltraBasics:
    def __init__(self):
        
        self.MB = MilkBasics()
        self.df      = pd.DataFrame()
        
        self.data = self.MB.dataLoader()  

        self.first_calf = self.create_first_calf()
        [self.last_calf, 
            self.lastcalf_age]                  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()
        self.IUB_dash_vars = self.get_dash_vars()
        
        
    
    def create_first_calf(self):
        
        first_calf1 = self.data['lb'].groupby('WY_id').agg({
            'b_date'  : 'min',
            'calf#'   : 'min'
            }).reset_index()
        
        self.first_calf = first_calf1.reindex(self.data['WY_ids'])
        # self.first_calf['WY_id'] = self.data['bd']['WY_id']     
           
        self.first_calf.rename(columns={'calf#': 'first calf#',
            'b_date': 'first calf bdate'}, inplace=True)     
        return self.first_calf

    

    def create_last_calf(self):
        
        last_calf1 = self.data['lb'].groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        
        last_calf1 = last_calf1.reindex(self.data['WY_ids'])
        # last_calf1['WY_id'] = self.data['bd']['WY_id']
        
        self.last_calf = last_calf1.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'})
        
        self.last_calf = self.last_calf.fillna({'last calf#': 0})
        self.lastcalf_age = [(tdy - date).days for date in self.last_calf['last calf bdate']]
        self.last_calf['last calf age'] = self.lastcalf_age
        
        # print(self.last_calf.iloc[92:96,:])
        
        return self.last_calf, self.lastcalf_age
    


    def create_last_stop(self):
        last_stop1 = self.data['stopx'].groupby('WY_id').agg({
            'lact_num'    : 'max',
            'stop'        : 'max'
        })  .reset_index() 

        self.last_stop = last_stop1.reindex(self.data['WY_ids'])
        # print(self.last_stop.iloc[240:250,:])
        self.last_stop = self.last_stop.rename(columns = {'lact_num':'stop calf#','stop':'last stop date'})       
        self.last_stop = self.last_stop.fillna({'last stop date': np.nan})
        

        
        return self.last_stop
    
    def get_dash_vars(self):
        self.IUB_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}

        return self.IUB_dash_vars

if __name__ == "__main__":
    iub=InsemUltraBasics()
    IUB_dash_vars = iub.get_dash_vars()
    
