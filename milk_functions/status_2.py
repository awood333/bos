'''status_2.py'''

import pandas as pd
import numpy as np
from CreateStartDate import DateRange
from insem_functions.InsemUltraBasics import InsemUltraBasics
from MilkBasics import MilkBasics

class StatusData2:
    
    def __init__(self):
        
        IUB = InsemUltraBasics()
        self.data = MilkBasics().data

        self.days_milking1 = IUB.last_calf.loc[:,'last calf age']
        DR = DateRange()
        self.startdate  = DR.startdate
        self.rng        = DR.date_range_daily

        f        = self.data['milk']
        f.index = pd.to_datetime(f.index)
        self.f1 = f.loc[self.startdate:,:].copy()
        self.last_milking   = self.f1.iloc[-1]
        
        self.wy_range = range(1,(len(self.data['bd'])+1))


        # functions

        [self.alive_df, self.gone_df, 
         self.milkers_df]                  = self.create_masks()
        

        
        [self.alive_ids, self.gone_ids, 
        self.milkers_ids, self.dry_ids]         = self.create_id_lists()
        
        self.dry, self.goners, self.milkers     = self.create_df()
        
        [self.alive_count, self.gone_count, 
         self.milkers_count, self.dry_count]    = self.create_count_lists()
        
        self.status_col                         = self.create_status_col()
        self.create_write_to_csv()
# 
    def create_masks(self):   
        self.alive_df      = self.data['bd'][self.data['bd']['death_date'].isnull()]
        self.gone_df       = self.data['bd'][self.data['bd']['death_date'].notnull()]
        self.milkers_df    = self.last_milking > 0

        return self.alive_df, self.gone_df, self.milkers_df
    


    def create_id_lists(self):
        self.alive_ids   = self.alive_df['WY_id'].to_list()
        self.gone_ids    = self.gone_df['WY_id'].to_list()
        self.milkers_ids = self.last_milking.loc[self.milkers_df].index.astype(int).tolist()
        self.dry_ids     = [id for id in self.alive_ids if id not in self.milkers_ids]
        return  self.alive_ids, self.gone_ids, self.milkers_ids, self.dry_ids
        


    def create_df(self):
        self.milkers             = pd.DataFrame(self.milkers_ids, columns=['ids'])
        self.milkers['status']   = "M"
        self.dry                 = pd.DataFrame(self.dry_ids, columns=['ids'])
        self.dry['status']       = 'D'
        self.goners              = pd.DataFrame(self.gone_ids, columns=['ids'])
        self.goners['status']    = 'G'
        return self.dry, self.goners, self.milkers


    def create_count_lists(self):
        self.alive_count     = len(self.alive_ids)
        self.gone_count      = len(self.gone_ids)
        self.milkers_count   = len(self.milkers_ids)
        self.dry_count       = len(self.dry_ids)
        
        return [
        self.alive_count, self.gone_count, 
        self.milkers_count, self.dry_count]
        

    def create_status_col(self):
        
        self.milkers = self.milkers.reset_index(drop=True)
        self.goners = self.goners.reset_index(drop=True)
        self.dry    = self.dry.reset_index(drop=True)
        
        status_col1 = pd.concat([self.milkers, self.goners, self.dry], axis=0)
        status_col2 = status_col1.sort_values(by='ids')
        self.status_col = status_col2.reset_index(drop=True)
        
        return self.status_col 
    
        
    def create_write_to_csv(self):
        self.status_col .to_csv('F:\\COWS\\data\\status\\status_col.csv')
        
if __name__ == "__main__":
    status_data2 = StatusData2()
    