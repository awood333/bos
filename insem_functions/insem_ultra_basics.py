'''InsemUltraBasics.py'''
import inspect
import pandas as pd
import numpy as np
from container import get_dependency
# from persistent_container_service import ContainerClient


tdy = pd.Timestamp.today()


class InsemUltraBasics:
    def __init__(self):

        print(f"InsemUltraBasics instantiated by: {inspect.stack()[1].filename}")

        self.DR = None
        self.MB = None
        self.data = None
        self.df = None
        self.lbpiv = None
        self.first_calf = None
        self.last_calf = None
        self.last_stop = None

    def load_and_process(self):
        self.DR = get_dependency('date_range')
        self.MB = get_dependency('milk_basics')
        self.MB.load_and_process()
        self.data = self.MB.data

        self.df = pd.DataFrame()
        self.lbpiv = self.create_livebirths_pivot()
        self.first_calf = self.create_first_calf()
        self.last_calf  = self.create_last_calf()
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
        
        last_calf2 = last_calf1.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'})
        
        last_calf3 = last_calf2.fillna({'last calf#': 0})
        # lastcalf_age = [(tdy - date).days for date in last_calf3['last calf bdate']]
        last_calf3['last calf age'] = [(tdy - date).days for date in last_calf3['last calf bdate']]
        
        self.last_calf = last_calf3
        
        return self.last_calf
    


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
        self.lbpiv.to_csv('F:\\COWS\\data\\insem_data\\livebirths_piv.csv')


if __name__ == "__main__":
    obj=InsemUltraBasics()
    obj.load_and_process() 
    
