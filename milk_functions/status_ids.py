'''milk_functions\\status_ids.py'''

import pandas as pd

from CreateStartDate import DateRange
from insem_functions.InsemUltraBasics import InsemUltraBasics
from MilkBasics import MilkBasics


class StatusData:
    
    def __init__(self):
                

        self.DR     = DateRange()
        self.IUB     = InsemUltraBasics()
        self.data = MilkBasics().data
             
        self.startdate = self.data['start']
        self.enddate = self.DR.enddate_monthly
        
        
        self.f1         = self.data['milk']
        self.datex      = self.data['milk'].index  
    
        self.stopdate       = self.f1.index.max() 
        self.bdmax          = len(self.data['bd'])
        self.wy_series      = pd.Series(list(range(1, self.bdmax + 1)), name='WY_id', index=range(1, self.bdmax + 1))
        
        
        # functions        
 

        [self.milkers, self.non_milkers, 
         self.milkers_count, self.non_milkers_count]        = self.create_milking_list()
         
        self.id_list_data                                   = self.create_id_lists()
        
        self.find_alive_gone_duplicates()
        self.find_milker_dry_duplicates()
        self.herd_df                                        = self.create_herd_list()
        self.herd_monthly                                   = self.create_herd_list_monthly()
        self.status_ids_dash_vars                           = self.get_dash_vars()

        self.create_write_to_csv()



    # sets the time frame

    def create_milking_list(self):
        
        self.milkers, self.non_milkers = [],[]
        self.milkers_count, self.non_milkers_count = [],[]
        
        for date in self.datex:
            milk_row = self.f1.loc[date,:]
            
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
            condition1 = self.data['bd']['birth_date'] <=date
            condition2 = self.data['bd']['death_date'] > date 
            condition3 = self.data['bd']['adj_bdate'] > date
            condition4 = self.data['bd']['death_date'] <= date
            condition5 = self.data['bd']['death_date'].isnull()
            
            alive1, gone1, dry_count1 = [],[],[]

            alive1 = self.data['bd'][((condition1 | condition3)
                             & (condition2 | condition5)
                             )
                             ].index.to_list()
            
            alive1_bool = self.data['bd'][((condition1 | condition3)
                  & (condition2 | condition5)
                 )].index.notna().tolist()
            
            self.alive_mask.append(alive1_bool)
            self.alive_ids.append(alive1)
            self.alive_count.append(len(alive1))
            
            gone1 = self.data['bd'][condition4].index.to_list()
            self.gone_ids.append(gone1)
            self.gone_count.append(len(gone1))
            
            dry_count1 = self.non_milkers_count[i] - self.gone_count[i]
            self.dry_count.append(dry_count1)
            
        self.id_list_data = [self.alive_ids, self.alive_count, 
                             self.alive_mask,self.gone_ids, 
                             self.gone_count, self.dry_count] 
        
        return self.id_list_data

 
    def find_alive_gone_duplicates(self):
        
        if not self.alive_ids or not self.gone_ids:
                print("The alive or the gone lists are empty.")
                return
            
        alive_gone_duplicates_list = []
        
        for alive_row, gone_row in zip(self.alive_ids, self.gone_ids):
            # Convert to sets
            alive_set = set(alive_row)
            gone_set = set(gone_row)
            
            # Find intersections
            alive_gone_duplicates = alive_set.intersection(gone_set)
            
            # Append duplicates to the list
            alive_gone_duplicates_list.append(alive_gone_duplicates)
            
        # Create a list of non-empty duplicates
        non_empty_duplicates_list = [duplicates for duplicates in alive_gone_duplicates_list if duplicates]
   
        
        # Print non-empty duplicates
        for i, duplicates in enumerate(non_empty_duplicates_list):
            print(f"alive_gone duplicates (row {i}):", duplicates)
            
        return alive_gone_duplicates_list
    
    
    
    def find_milker_dry_duplicates(self):
        
        if not self.milkers or not self.non_milkers:
            print("The milkers or the non-milkers lists are empty.")
            return
            
        milkers_non_milkers_duplicates_list = []
        
        for milkers_row, non_milkers_row in zip(self.milkers, self.non_milkers):
            # Convert to sets
            milkers_set = set(milkers_row)
            non_milkers_set = set(non_milkers_row)
            
            # Find intersections
            milkers_non_milkers_duplicates = milkers_set.intersection(non_milkers_set)
            
            # Append duplicates to the list
            milkers_non_milkers_duplicates_list.append(milkers_non_milkers_duplicates)
            
        # Create a list of non-empty duplicates
        non_empty_duplicates_list = [duplicates for duplicates in milkers_non_milkers_duplicates_list if duplicates]
   
        
        # Print non-empty duplicates
        for i, duplicates in enumerate(non_empty_duplicates_list):
            print(f"milkers_non_milkers_ duplicates (row {i}):", duplicates)
            
        return milkers_non_milkers_duplicates_list

 
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
    
    def create_herd_list_monthly(self):
        
        self.herd_df.index = pd.to_datetime(self.herd_df.index)
        
        hm = self.herd_df.groupby(pd.Grouper(freq='ME')).mean()
        
        hm['year'] = hm.index.year
        hm['month'] = hm.index.month
    
        # Set year and month as a multi-index
        hm.set_index(['year', 'month'], inplace=True)
    
        
        self.herd_monthly = hm
        return self.herd_monthly
    
    def get_dash_vars(self):
        
        self.status_ids_dash_vars = {
            'milk series' : self.f1,
            'date series' : self.datex,
            'herd monthly' : self.herd_monthly
            
            
        }
        
        # self.status_ids_dash_vars = {name: value for name, value in vars(self).items()
        #        if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.status_ids_dash_vars  

        

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
    
if __name__ == '__main__':
    SD = StatusData()
