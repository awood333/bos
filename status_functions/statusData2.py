'''status_2.py'''

import pandas as pd
# import numpy as np
from CreateStartDate import DateRange
from insem_functions.insem_ultra_basics import InsemUltraBasics
from MilkBasics import MilkBasics

class StatusData2:
    
    def __init__(self):
        
        IUB = InsemUltraBasics()
        self.MB = MilkBasics() 

        self.days_milking1 = IUB.last_calf.loc[:,'last calf age']
        DR = DateRange()
        self.startdate  = DR.startdate
        self.rng        = DR.date_range_daily
        self.milk = self.MB.data['milk']
        

        self.last_milking   = self.MB.data['milk'].iloc[-1:,:]


        # functions

        self.masks          = self.create_masks()
        self.id_lists       = self.create_id_lists()
        self.list_of_dfs    = self.create_df()
        self.count_lists    = self.create_count_lists()
        self.status_col                         = self.create_status_col()
        self.create_write_to_csv()
# 
    def create_masks(self):   
        self.alive_mask      = self.MB.data['bd'][self.MB.data['bd']['death_date'].isnull()]
        self.gone_mask       = self.MB.data['bd'][self.MB.data['bd']['death_date'].notnull()]
        self.milkers_mask    = self.MB.data['milk'].iloc[-1,:] > 0

        self.masks = [self.alive_mask, self.gone_mask, self.milkers_mask]
        return self.masks


    def create_id_lists(self):
        self.alive_ids   = self.alive_mask['WY_id'].to_list()
        self.gone_ids    = self.gone_mask['WY_id'].to_list()
        self.milkers_ids = self.last_milking.columns[self.milkers_mask].astype(int).tolist() 
        self.dry_ids     = [id for id in self.alive_ids if id not in self.milkers_ids]
        
        self.id_lists = [self.alive_ids, self.gone_ids, self.milkers_ids, self.dry_ids]
        return self.id_lists


    def create_df(self):
        self.milkers             = pd.DataFrame(self.milkers_ids, columns=['ids'])
        self.milkers['status']   = "M"
        self.dry                 = pd.DataFrame(self.dry_ids, columns=['ids'])
        self.dry['status']       = 'D'
        self.goners              = pd.DataFrame(self.gone_ids, columns=['ids'])
        self.goners['status']    = 'G'
        
        self.list_of_dfs = [self.dry, self.goners, self.milkers]
        return self.list_of_dfs


    def create_count_lists(self):
        self.alive_count     = len(self.alive_ids)
        self.gone_count      = len(self.gone_ids)
        self.milkers_count   = len(self.milkers_ids)
        self.dry_count       = len(self.dry_ids)
        
        self.count_lists = [
        self.alive_count, self.gone_count, 
        self.milkers_count, self.dry_count]
        return self.count_lists
        

    def create_status_col(self):
        
        self.milkers = self.milkers.reset_index(drop=True)
        self.goners = self.goners.reset_index(drop=True)
        self.dry    = self.dry.reset_index(drop=True)
        
        status_col1 = pd.concat([self.milkers, self.goners, self.dry], axis=0)
        status_col2 = status_col1.sort_values(by='ids')
        self.status_col = status_col2.reset_index(drop=True)
       
        return self.status_col 
    
        
    def create_write_to_csv(self):
        milkers_ids_df = pd.DataFrame(self.milkers_ids, columns=['ids'])
        dry_ids_df     = pd.DataFrame(self.dry_ids)
        
        
        milkers_ids_df .to_csv('F:\\COWS\\data\\status\\milkers_ids.csv')
        dry_ids_df     .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')
        self.status_col  .to_csv('F:\\COWS\\data\\status\\status_col.csv')
    
if __name__ == "__main__":
    SD = StatusData2()
