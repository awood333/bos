'''milk_functions\\status_ids.py'''

import pandas as pd

from feed_related.CreateStartDate import DateRange
from insem_functions.InsemUltraBasics import InsemUltraBasics


class StatusData:
    
    def __init__(self):
        
                
        IUB     = InsemUltraBasics()
        DR     = DateRange()
             
        self.lb = IUB.create_last_calf()
        self.startdate = DR.startdate
        self.enddate = DR.enddate_monthly
        
        self.f1          = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0,  header=0)
        self.f1.index    = pd.to_datetime(self.f1.index)
        
        self.f          = self.f1.loc[self.startdate:,:].copy()
    
        self.bd         = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',      index_col=0, header=0)
        date_fields=['birth_date','death_date','arrived', 'adj_bdate']
        
        for date_field in date_fields:
            self.bd[date_field] = pd.to_datetime(self.bd[date_field])

        self.maxdate        = self.f1.index.max() 
        self.stopdate       = self.maxdate 
        self.bdmax          = len(self.bd)
        self.wy_series      = pd.Series(list(range(1, self.bdmax + 1)), name='WY_id', index=range(1, self.bdmax + 1))
        
        
        # functions        
        self.f, self.datex                                  = self.create_partition_milk_df()     

        [self.milkers, self.non_milkers, 
         self.milkers_count, self.non_milkers_count]        = self.create_milking_list()
         
        [self.alive_ids, self.alive_count, self.alive_mask,
        self.gone_ids, self.gone_count, 
        self.dry_count]                                     = self.create_id_lists()
        
        self.find_duplicates()
        self.herd_df                                        = self.create_herd_list()

        self.create_write_to_csv()

    
    def create_partition_milk_df(self):
        self.f = self.f1.loc[self.startdate:self.enddate, :].copy()     # Note - enddate is last day of last completed month
        self.datex          = self.f.index.to_list()
        return self.f, self.datex
    
    def create_milking_list(self):
        
        self.milkers, self.non_milkers = [],[]
        self.milkers_count, self.non_milkers_count = [],[]
        
        for date in self.datex:
            milk_row = self.f.loc[date,:]
            
            milking_mask =  milk_row>0
            not_milking_mask =  pd.isna(milk_row)
            
            len_milkers1 = sum(milking_mask)
            len_non_milkers1 = sum(not_milking_mask)
            
            milker1 = milk_row.index[milking_mask]
            non_milker1 = milk_row.index[not_milking_mask]
            
            self.milkers.append(milker1)
            self.non_milkers.append(non_milker1)
            
            self.milkers_count.append(len_milkers1)
            self.non_milkers_count.append(len_non_milkers1)
            
            milking_mask=[]
            not_milking_mask=[]
            milker1=[]
            len_milkers1=[]
            len_non_milkers1=[]
            
        return self.milkers, self.non_milkers, self.milkers_count, self.non_milkers_count
    
    
    def create_id_lists(self):   
        self.alive_ids,  self.alive_count, self.alive_mask =[],[],[]
        self.gone_ids,   self.gone_count  =[],[]
        self.nby_ids,    self.nby_count   =[],[]
        self.dry_count = []
        
        for i, date in enumerate(self.datex):
            condition1 = self.bd['birth_date'] <=date
            condition2 = self.bd['death_date'] > date 
            condition3 = self.bd['adj_bdate'] > date
            condition4 = self.bd['death_date'] <= date
            condition5 = self.bd['death_date'].isnull()
            
            
            alive1, gone1, dry_count1 = [],[],[]
    
            
        
            alive1 = self.bd[((condition1 | condition3)
                             & (condition2 | condition5)
                             )
                             ].index.to_list()
            
            alive1_bool = self.bd[((condition1 | condition3)
                  & (condition2 | condition5)
                 )].index.notna().tolist()
            
            self.alive_mask.append(alive1_bool)
            self.alive_ids.append(alive1)
            self.alive_count.append(len(alive1))
            
            gone1 = self.bd[condition4].index.to_list()
            self.gone_ids.append(gone1)
            self.gone_count.append(len(gone1))
            
            dry_count1 = self.non_milkers_count[i] - self.gone_count[i]
            self.dry_count.append(dry_count1)
            
            
        return  [self.alive_ids, self.alive_count, self.alive_mask,
        self.gone_ids, self.gone_count, 
        self.dry_count] 

 
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
            'milkers': self.milkers_count,
            'dry':      self.dry_count,
            'gone': self.gone_count,
        }
        
        self.herd_df = pd.DataFrame(data, index=self.datex)
        
        self.herd_df['total'] = (
            self.herd_df['alive'] + self.herd_df['gone']
            )
        
        return self.herd_df
        

    def create_write_to_csv(self):
        
        self.herd_df    .to_csv('F:\\COWS\\data\\status\\herd_df.csv')
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
