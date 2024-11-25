'''milk_functions\\status_ids.py'''

import pandas as pd

from CreateStartDate import DateRange
# from insem_functions.insem_ultra_basics import InsemUltraBasics
from MilkBasics import MilkBasics


class StatusData:
    
    def __init__(self):

        self.MB = MilkBasics()                

        self.CSD =  DateRange()
             
        # self.lb = self.insem_ultra_basics.create_last_calf()
        self.startdate = self.CSD.startdate
        self.enddate = self.CSD.enddate_monthly
        
        f1          = self.MB.data['milk']
        self.f         = f1.loc[self.startdate:,:].copy()

        
        
        self.maxdate        = self.f.index.max() 
        self.stopdate       = self.maxdate 

        self.bd1             = self.MB.data['bd']
        self.bdmax          = len(self.bd1)

        
        self.wy_series      = pd.Series(list(range(1, self.bdmax + 1)), name='WY_id', index=range(1, self.bdmax + 1))
        
        # functions        
       
        self.milkers_lists            = self.create_milking_list()
        self.id_lists_data            = self.create_id_lists()
        self.dry_ids, self.dry_count  = self.create_dry_id_list()
        
        self.find_duplicates()
        self.herd_df                  = self.create_herd_list()
        self.herd_list_monthly        = self.create_herd_list_monthly()

        self.create_write_to_csv()

    

    def create_milking_list(self):
        
        self.milker_ids, self.non_milker_ids = [],[]
        self.milkers_count, self.non_milkers_count = [],[]
        datex = self.f.index
        
        for date in datex:
            milk_row = self.f.loc[date,:]
            
            milking_mask =  milk_row>0
            not_milking_mask =  pd.isna(milk_row)
            
            len_milkers1 = sum(milking_mask)
            len_non_milkers1 = sum(not_milking_mask)
            
            milker1 = milk_row.index[milking_mask]
            non_milker1 = milk_row.index[not_milking_mask]
            
            self.milker_ids.append(milker1)
            self.non_milker_ids.append(non_milker1)
            
            self.milkers_count.append(len_milkers1)
            self.non_milkers_count.append(len_non_milkers1)
            
            milking_mask=[]
            not_milking_mask=[]
            milker1=[]
            len_milkers1=[]
            len_non_milkers1=[]
        
        self.milkers_lists = [self.milker_ids, self.non_milker_ids, self.milkers_count, self.non_milkers_count]
            
        return self.milkers_lists
    
    
    def create_id_lists(self):   
        self.alive_ids,  self.alive_count, self.alive_mask =[],[],[]
        self.gone_ids,   self.gone_count  =[],[]
        self.nby_ids,    self.nby_count   =[],[]
        self.dry_count = []
        
        bd = self.bd1.set_index('WY_id')
        bd.index = bd.index.astype(str)
        
        for date in self.f.index:
            condition1 = bd['birth_date'] <=date
            condition2 = bd['death_date'] > date 
            condition3 = bd['adj_bdate'] > date
            condition4 = bd['death_date'] <= date
            condition5 = bd['death_date'].isnull()
            
            
            alive1, gone1 = [],[]
    
            
        
            alive1 = bd[((condition1 | condition3)
                             & (condition2 | condition5)
                             )
                             ].index.to_list()
            
            alive1_bool = bd[((condition1 | condition3)
                  & (condition2 | condition5)
                 )].index.notna().tolist()
            
            self.alive_mask.append(alive1_bool)
            self.alive_ids.append(alive1)
            self.alive_count.append(len(alive1))
            
            gone1 = bd[condition4].index.to_list()
            self.gone_ids.append(gone1)
            self.gone_count.append(len(gone1))
            
            
            self.id_lists_data = [
                self.alive_ids, self.alive_count, 
                self.alive_mask,
                self.gone_ids, self.gone_count 
                ] 
            
        return self.id_lists_data 
    
    
    def create_dry_id_list(self):
        m1 = self.milker_ids
        a1 = self.alive_ids
        self.dry_ids, self.dry_count = [],[]
        
        for alive, milker_ids in zip(a1, m1):
            d1 = [id for id in alive if id not in milker_ids]
            self.dry_ids.append(d1)
            self.dry_count.append(len(d1))             
        
        return self.dry_ids, self.dry_count
 
    def find_duplicates(self):
        if not self.alive_ids or not self.gone_ids:
            print("One or more lists are empty.")
            return
        
        # Extract the first row
        alive_first_row = self.alive_ids[0]
        gone_first_row = self.gone_ids[0]

        # Convert to sets
        alive_set = set(alive_first_row)
        gone_set = set(gone_first_row)
        
        # Find intersections
        alive_gone_duplicates = alive_set.intersection(gone_set)
        
        # Print duplicates
        print("Duplicates between alive and gone (first row):", alive_gone_duplicates)

 
    def create_herd_list(self):
        data = {
            'alive': self.alive_count,
            'milker_ids': self.milkers_count,
            'dry':      self.dry_count
            # 'gone': self.gone_count,
        }
        
        self.herd_df = pd.DataFrame(data, index=self.f.index)
        
        return self.herd_df
    
    def create_herd_list_monthly(self):
        
        self.herd_df.index = pd.to_datetime(self.herd_df.index)
        hm = self.herd_df.groupby(pd.Grouper(freq='ME')).mean()
        
        hm['year'] = hm.index.year
        hm['month'] = hm.index.month
    
        # Set year and month as a multi-index
        hm.set_index(['year', 'month'], inplace=True)
        self.herd_list_monthly = hm
        
        return self.herd_list_monthly
    
        

    def create_write_to_csv(self):
        
        self.herd_df    .to_csv('F:\\COWS\\data\\status\\herd_df.csv')
        self.herd_list_monthly   .to_csv('F:\\COWS\\data\\status\\herd_list_monthly.csv')        

if __name__ =="__main__":
    status_data = StatusData()