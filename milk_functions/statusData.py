'''milk_functions\\statusData.py'''

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
       
        # self.create_alive_mask()
       
        [self.milker_ids, self.dry_ids, 
         self.milker_count, self.dry_count]  = self.create_milking_list()
        
        self.milker_ids_df, self.dry_ids_df  = self.create_df()
        
        # [self.alive_ids, self.alive_count, self.alive_mask,
        # self.gone_ids, self.gone_count 
        # ]                                   = self.create_id_lists()
        
        # self.dry_ids, self.dry_count        = self.create_dry_id_list()
        
        # self.find_duplicates()
        self.herd_daily          = self.create_herd_daily()
        self.herd_monthly        = self.create_herd_monthly()

        self.create_write_to_csv()


    def create_milking_list(self):
        
        alive2=[]        
        self.milker_ids, self.dry_ids = [],[]
        self.milker_count, self.dry_count = [],[]
        datex = self.f.index
        bd  = self.bd1['birth_date']
        bd.index +=1
        bd.index = bd.index.astype(str)
        dd = self.bd1['death_date']   
        dd.index +=1
        dd.index = dd.index.astype(str)
        

        
        for date in datex:
            
            for i in self.f.columns:
                alive1 = date > bd.loc[i] and (date < dd.loc[i] or pd.isna( dd.loc[i] ))
                alive2.append(alive1)
            
            milk_row = self.f.loc[date,alive2]
            
            milking_mask =  milk_row>0
            dry_mask     =  pd.isna(milk_row)
            
            len_milker1 = sum(milking_mask)
            len_dry1     = sum(dry_mask)
            
            milker1 = milk_row.index[milking_mask]
            dry1    = milk_row.index[dry_mask]
            
            self.milker_ids .append(milker1)
            self.dry_ids    .append(dry1)
            
            self.milker_count.append(len_milker1)
            self.dry_count   .append(len_dry1)
            
            
            alive2 = []
            milking_mask=[]
            dry_mask=[]
            milker1=[]
            len_milker1=[]
            len_dry1=[]
            
        return [self.milker_ids, self.dry_ids, self.milker_count, self.dry_count]
    
    def create_df (self):
        
        df = pd.DataFrame(self.milker_ids)
        self.milker_ids_df = df.set_index(self.f.index)
        
        df2 = pd.DataFrame(self.dry_ids)
        self.dry_ids_df = df2.set_index(self.f.index)
       
        return self.milker_ids_df, self.dry_ids_df
    
    
    def create_herd_daily(self):
        data = {
            # 'alive': self.alive_count,
            'milkers': self.milker_count,
            'dry':      self.dry_count
            # 'gone': self.gone_count,
        }
        
        herd1 = pd.DataFrame(data, index=self.f.index)
        
        herd1['dry_15pct']  = (herd1['milkers'] * .15).to_frame(name = 'dry 15pct')    
        herd1['dry_85pct']  = (herd1['milkers'] * .85).to_frame(name = 'dry 85pct')
        self.herd_daily = herd1
        return   self.herd_daily
    
    
    def create_herd_monthly(self):
          
        hm = self.herd_daily.groupby(pd.Grouper(freq='ME')).mean()
        
        hm['year'] = hm.index.year
        hm['month'] = hm.index.month
    
        # Set year and month as a multi-index
        hm.set_index(['year', 'month'], inplace=True)
        self.herd_monthly = hm
        
        return self.herd_monthly
    

    def create_write_to_csv(self):

        self.milker_ids_df    .to_csv('F:\\COWS\\data\\status\\milker_ids.csv')
        self.dry_ids_df       .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')  
        self.herd_daily    .to_csv('F:\\COWS\\data\\status\\herd_daily.csv')
        self.herd_monthly   .to_csv('F:\\COWS\\data\\status\\herd_monthly.csv')        

if __name__ =="__main__":
    status_data = StatusData()