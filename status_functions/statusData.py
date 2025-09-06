'''milk_functions\\statusData.py'''
import inspect
import pandas as pd

from date_range import DateRange
from milk_basics import MilkBasics


class StatusData:
    
    def __init__(self, date_range=None, milk_basics=None):

        print(f"StatusData instantiated by: {inspect.stack()[1].filename}")
        
        self.MB = milk_basics or MilkBasics()                
        self.CSD = date_range or DateRange()
             
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
       
        [self.milker_ids, self.dry_ids, self.alive_ids,
        self.alive_count, self.milker_count, self.dry_count]  = self.create_milking_list()
        
        self.milker_ids_df, self.dry_ids_df  = self.create_df()
        
        self.herd_daily          = self.create_herd_daily()
        self.herd_monthly        = self.create_herd_monthly()

        self.create_write_to_csv()


    def create_milking_list(self):
        
        alive_mask, alive_ids, alive_series=[],[],[]
        self.milker_ids, self.dry_ids = [],[]
        self.alive_count, self.milker_count, self.dry_count = [],[],[]
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
                alive_mask.append(alive1)
            
            alive_series = self.f.loc[date,alive_mask].copy()  #series 
            
            milking_mask =  alive_series>0
            dry_mask     =  pd.isna(alive_series)
            
            len_milkers = sum(milking_mask)
            len_dry    = sum(dry_mask)
            len_alive   = sum(alive_mask)
            
            milker1 = alive_series.index[milking_mask]
            dry1    = alive_series.index[dry_mask]
            
            self.alive_ids = alive_series.index
            self.milker_ids .append(milker1)
            self.dry_ids    .append(dry1)
            
            self.alive_count .append(len_alive)
            self.milker_count.append(len_milkers)
            self.dry_count   .append(len_dry)
            
            
            alive_mask = []
            milking_mask=[]
            dry_mask=[]
            milker1=[]
            len_milkers=[]
            len_dry=[]
            len_alive=[]
            
        return [self.milker_ids, self.dry_ids, self.alive_ids,
                self.alive_count, self.milker_count, self.dry_count]
    
    def create_df (self):
        
        df = pd.DataFrame(self.milker_ids)
        self.milker_ids_df = df.set_index(self.f.index)
        
        df2 = pd.DataFrame(self.dry_ids)
        self.dry_ids_df = df2.set_index(self.f.index)
       
        return self.milker_ids_df, self.dry_ids_df
    
    
    def create_herd_daily(self):
        data = {
            'alive': self.alive_count,
            'milkers': self.milker_count,
            'dry':      self.dry_count
            # 'gone': self.gone_count,
        }
        
        herd1 = pd.DataFrame(data, index=self.f.index)
        
        herd1['dry_15pct']  = (herd1['milkers'] * .15).to_frame(name = 'dry 15pct')    
        # herd1['dry_85pct']  = (herd1['milkers'] * .85).to_frame(name = 'dry 85pct')
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

        self.milker_ids_df  .to_csv('F:\\COWS\\data\\status\\milker_ids.csv')
        self.dry_ids_df     .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')  
        self.herd_daily     .to_csv('F:\\COWS\\data\\status\\herd_daily.csv')
        self.herd_monthly   .to_csv('F:\\COWS\\data\\status\\herd_monthly.csv')        

if __name__ =="__main__":
    status_data = StatusData()