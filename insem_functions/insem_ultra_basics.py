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
        self.lbpiv = self.create_livebirths_pivot()  

        self.first_calf = self.create_first_calf()
        [self.last_calf, 
            self.lastcalf_age]                  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()
        self.write_to_csv()
        
        
    def create_livebirths_pivot(self):
        
        lb1 = self.MB.lb[['WY_id','b_date','calf#','try#']].copy()
        
        # Fill NaN values in 'calf#' and 'try#' with placeholders (e.g., 0 or 'missing')
        lb1['calf#'] = lb1['calf#'].fillna(0).astype(int, errors='ignore')
        lb1['try#'] = lb1['try#'].fillna(0).astype(int, errors='ignore')
                
        
        self.lbpiv = pd.pivot_table(lb1,
                               index='WY_id',
                               columns='calf#',
                               values='b_date',
                               dropna=False #retain rows with NaT vals
                               )
        
        if (0, 0) in self.lbpiv.columns:
            self.lbpiv = self.lbpiv.drop(columns=(0, 0))
            
        return self.lbpiv
        
        
    
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
    
    def write_to_csv(self):
        self.lbpiv.to_csv('F:\\COWS\\data\\insem_data\\lbpiv.csv')


if __name__ == "__main__":
    InsemUltraBasics()

    
