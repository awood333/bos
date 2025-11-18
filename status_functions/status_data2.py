'''status_2.py'''
import inspect
import pandas as pd
from container import get_dependency


class StatusData2:
    def __init__(self):

        print(f"StatusData2 instantiated by: {inspect.stack()[1].filename}")

        self.MB = None
        self.DR = None
        self.startdate = None
        self.rng = None
        self.milk = None
        self.last_milking = None

        #create masks
        self.alive_ids = None
        self.gone_ids = None
        self.milkers_mask = None
        self.masks = None

        #create_id_lists
        self.alive_ids = None
        self.gone_ids = None
        self.milkers_ids = None
        self.dry_ids = None                
        self.id_lists = None

        #create_df
        self.list_of_dfs = None
        self.count_lists = None
        self.status_col = None

        #create_count_lists  
        self.count_lists = None

        #self.count_lists
        self.count_lists = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.startdate = self.DR.startdate
        self.rng = self.DR.date_range_daily
        self.milk = self.MB.data['milk']
        self.last_milking = self.MB.data['milk'].iloc[-1:, :]

        self.masks = self.create_masks()
        self.id_lists = self.create_id_lists()
        self.list_of_dfs = self.create_df()
        self.count_lists = self.create_count_lists()
        self.status_col = self.create_status_col()
        self.create_write_to_csv()


    def create_masks(self):
        bd = self.MB.data['bd'].reset_index(drop=True)  # Ensure integer index
        mask = bd['death_date'].isnull()
        self.alive_mask = bd[mask]
        self.gone_mask = bd[~mask]
        self.milkers_mask = self.MB.data['milk'].iloc[-1, :] > 0
        self.masks = (self.alive_mask, self.gone_mask, self.milkers_mask)
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
        
        self.list_of_dfs = (self.dry, self.goners, self.milkers)
        return self.list_of_dfs


    def create_count_lists(self):
        self.alive_count     = len(self.alive_ids)
        self.gone_count      = len(self.gone_ids)
        self.milkers_count   = len(self.milkers_ids)
        self.dry_count       = len(self.dry_ids)
        
        self.count_lists = (
        self.alive_count, self.gone_count, 
        self.milkers_count, self.dry_count)
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
    obj = StatusData2()
    obj.load_and_process()
