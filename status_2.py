'''
status.py
'''
import pandas as pd
import numpy as np

class StatusData:
    
    def __init__(self):
        self.f1        = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0,  header=0, 
                            parse_dates=['datex'])
        
        self.bd        = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',      index_col=0, header=0,
                            parse_dates=['birth_date','death_date','arrived', 'adj_bdate'])

        self.last_milking   = self.f1.iloc[-1]

        # functions

        self.alive_mask, self.milkers_mask                      = self.create_masks()
        self.alive_ids, self.milkers_ids, self.dry_ids          = self.create_id_lists()
        self.dry, self.milkers                                  = self.create_df()
        self.alive_count, self.milkers_count, self.dry_count    = self.create_count_lists()
        self.status_col                                         = self.create_status_col()
        self.create_write_to_csv()
# 
    def create_masks(self):   
        alive_mask      = self.bd[self.bd['death_date'].isnull()]
        milkers_mask    = self.last_milking > 0   
        return alive_mask, milkers_mask


    def create_id_lists(self):
        alive_ids   = self.alive_mask.index.to_list()
        milkers_ids = self.last_milking.loc[self.milkers_mask].index.astype(int).tolist()
        dry_ids     = [id for id in alive_ids if id not in milkers_ids]
        return  alive_ids, milkers_ids, dry_ids


    def create_df(self):
        milkers             = pd.DataFrame(self.milkers_ids, columns=['ids'])
        milkers['status']   = "M"
        dry                 = pd.DataFrame(self.dry_ids, columns=['ids'])
        dry['status']       = 'D'
        return milkers, dry


    def create_count_lists(self):
        alive_count     = len(self.alive_ids)
        milkers_count   = len(self.milkers_ids)
        dry_count       = len(self.dry_ids)
        return alive_count, milkers_count, dry_count
        

    def create_status_col(self):
        status_col = pd.concat([self.milkers, self.dry], axis=0).sort_values(by='ids').reset_index(drop=True)
        return status_col
    
        
    def create_write_to_csv(self):
        self.status_col .to_csv('F:\\COWS\\data\\status\\status_col.csv')
    
    
if __name__ == "__main__":
    status_data = StatusData()
    # print(status_data.alive_ids)
    print('milker',status_data.milkers_ids)
    print('dry',status_data.dry_ids)
    print('status col',status_data.status_col)

        
