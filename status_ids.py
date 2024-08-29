'''
status.py
'''
import pandas as pd
import numpy as np
from startdate_funct import CreateStartdate
from insem_ultra import InsemUltraData

class StatusDataLong:
    
    def __init__(self):
        self.f1     = pd.read_csv('F:\\COWS\\data\\csv_files\\xlformat\\fullday.csv',index_col='datex')
        self.bd     = pd.read_csv('F:\\COWS\\data\\csv_files\\xlformat\\birth_death.csv')
        

        IUD    = InsemUltraData()
        self.lb = IUD.create_last_calf()
        
        self.maxdate        = self.f1.index.max() 
        self.stopdate       = self.maxdate 
        self.bdmax          = len(self.bd)
        self.wy_series      = pd.Series(list(range(1, self.bdmax + 1)), name='WY_id', index=range(1, self.bdmax + 1))
        
        
        # functions        
        self.f, self.datex  = self.create_partition_milk_df()     
         
        [self.alive_ids, self.alive_count, 
        self.gone_ids, self.gone_count, 
        self.nby_ids, self.nby_count]    = self.create_id_lists()
        
        self.find_duplicates()
        self.herd_count   = self.create_dry_list()

        # self.create_write_to_csv()

    
    
    
    def create_partition_milk_df(self):
        self.f = self.f1.loc[:, :].copy()     # partitions the milk df
        self.datex          = self.f.index.to_list()
        return self.f, self.datex
        
    
    def create_id_lists(self):   
        self.alive_ids,  self.alive_count =[],[]
        self.gone_ids,   self.gone_count  =[],[]
        self.nby_ids,    self.nby_count   =[],[]
        
        for date in self.datex:
            condition1 = self.bd['birth_date'] <=date
            condition2 = self.bd['death_date'] > date 
            condition3 = self.bd['adj_bdate'] > date
            condition4 = self.bd['death_date'] <= date
            condition5 = self.bd['death_date'].isnull()
        
            alive1 = self.bd[(condition1) & (condition2 | condition5)]['WY_id'].to_list()
            self.alive_ids.append(alive1)
            self.alive_count.append(len(alive1))
            
            gone1 = self.bd[condition4]['WY_id'].to_list()
            self.gone_ids.append(gone1)
            self.gone_count.append(len(gone1))
            
            nby1 = self.bd[condition3]['WY_id'].to_list()
            self.nby_ids.append(nby1)
            self.nby_count.append(len(nby1))
            
        return  [self.alive_ids, self.alive_count, 
        self.gone_ids, self.gone_count, 
        self.nby_ids, self.nby_count] 

 
    def find_duplicates(self):
        if not self.alive_ids or not self.gone_ids or not self.nby_ids:
            print("One or more lists are empty.")
            return
        
        # Extract the first row
        alive_first_row = self.alive_ids[0]
        gone_first_row = self.gone_ids[0]
        nby_first_row = self.nby_ids[0]
        
        # Convert to sets
        alive_set = set(alive_first_row)
        gone_set = set(gone_first_row)
        nby_set = set(nby_first_row)
        
        # Find intersections
        alive_gone_duplicates = alive_set.intersection(gone_set)
        alive_nby_duplicates = alive_set.intersection(nby_set)
        gone_nby_duplicates = gone_set.intersection(nby_set)
        
        # Print duplicates
        print("Duplicates between alive and gone (first row):", alive_gone_duplicates)
        print("Duplicates between alive and nby (first row):", alive_nby_duplicates)
        print("Duplicates between gone and nby (first row):", gone_nby_duplicates)

 
    def create_dry_list(self):
        
        alive_count_df = pd.DataFrame({'alive_count': self.alive_count}, index=self.datex)
        gone_count_df  = pd.DataFrame({'gone_count': self.gone_count}, index=self.datex)
        nby_count_df   = pd.DataFrame({'nby_count': self.nby_count}, index=self.datex)
        
        
        herd_count=[]
        
        for date in self.datex:
            herd1 = alive_count_df.loc[date, 'alive_count'] + gone_count_df.loc[date, 'gone_count'] + nby_count_df.loc[date, 'nby_count']
            herd_count.append(herd1)
        return herd_count

    

        
        
if __name__ == "__main__":
    status_data = StatusDataLong()  
    
    
    # def create_write_to_csv(self):
        
    #     self.statuscows    .to_csv('F:\\COWS\\data\\status\\statuscows.csv')
    #     self.status_col .to_csv('F:\\COWS\\data\\status\\status_col.csv')
    #     self.milkers_ids.to_csv('F:\\COWS\\data\\status\\milkers_ids.csv')
        
    #     self.gone_ids   .to_csv('F:\\COWS\\data\\status\\gone_ids.csv')
    #     self.alive_ids  .to_csv('F:\\COWS\\data\\status\\alive_ids.csv')
    #     self.dry_ids    .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')
    #     self.nby_ids    .to_csv('F:\\COWS\\data\\status\\nby_ids.csv')
        
    #     self.group_a_ids  .to_csv('F:\\COWS\\data\\status\\group_a_ids.csv')
    #     self.group_b_ids  .to_csv('F:\\COWS\\data\\status\\group_b_ids.csv')
        

    #     self.days_milking_df    .to_csv('F:\\COWS\\data\\status\\days_milking.csv')    
    #     self.days_mean          .to_csv('F:\\COWS\\data\\status\\days_mean.csv')
        
    #     self.combined_status_cols.to_csv(('F:\\COWS\\data\\status\\combined_status_cols.csv'))
