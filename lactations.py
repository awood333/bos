'''
lactations.py
'''
 
import pandas as pd    #123   456
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as td
from insem_ultra import InsemUltraData

class Lactations:
    def __init__(self):

        self.start   = pd.read_csv    ('F:\\COWS\\data\\csv_files\\live_births.csv',     header = 0, parse_dates = ['b_date'])
        self.stop    = pd.read_csv    ('F:\\COWS\\data\\csv_files\\stop_dates.csv',      header = 0, parse_dates = ['stop'])
        self.milk   = pd.read_csv    ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', header = 0, index_col   = 'datex', parse_dates=['datex'])
        self.bd      = pd.read_csv    ('F:\\COWS\\data\\csv_files\\birth_death.csv',     header = 0, parse_dates = ['birth_date','death_date'])
        self.iud     = InsemUltraData()
        self.all    = self.iud.all
  
        self.max_milking_cownum  = self.milk.T.index.max()    # no heifers
        self.max_bd_cownum       = self.bd.index.max()        # including heifers
        self.cutoffdate = pd.to_datetime('2023-04-01',format='%Y-%m-%d')
      
        self.maxmilkdate =  pd.to_datetime(self.milk.index.max(),format='%m/%d/%Y')
    

        
        self.step7np, self.step7   = self.create_step7()
        self.cows, self.laststop   , self.lastcalf    , self.all1  , self.all2       = self.create_lastbirth()

        self.milk_df, self.cowlist = self.create_milk_df()
        self.lact_np, self.lact_df, self.colnames = self.create_array()
        
        self.lactwk        = self.create_weekly()
        self.write_to_csv()      
        
        
    def create_step7(self):
        wk      = np.repeat(np.arange(1,45), 7)
        step7x  = np.tile(np.arange(1,8),44)
        step7np = np.column_stack((wk, step7x))       # numpy array
        step7   = pd.DataFrame(step7np, columns=['wk', 'step7'])
        return step7np, step7
    
    def create_lastbirth(self):
        lastcalf = (self.all['last calf bdate'] [ self.all[  'last calf bdate'] > self.cutoffdate] ).to_frame(name='lastcalf')
        laststop = (self.all['last stop date'] [ (self.all['last stop date']) > self.all['last calf bdate']]).to_frame(name='laststop')
        dd      =  (self.all[ 'death_date'   ] [ (self.all['death_date'])     > self.all['last calf bdate']]).to_frame(name='dd')
        

         
        all2 = lastcalf.merge(laststop, how='left', left_index=True, right_index=True)
        all1 = all2.    merge(dd, how='left', left_index=True, right_index=True)
        
        
        all1['laststop'] = all1['laststop'].fillna(self.maxmilkdate) 
        cows = all1
        return cows, lastcalf, laststop, all1, all2
    
    def create_milk_df(self):
        cowlist = list(self.cows.index)
        milk_df = self.milk.iloc[:,cowlist]
        return milk_df, cowlist


    def create_array(self):
        lactx_list = []
        maxrows = 308
        
        
        
        startcol = self.cows['lastcalf']
        stopcol  = self.cows['laststop']
        
        startcol.index  = startcol.index.astype(str)
        stopcol.index   = stopcol.index.astype(str)
        colnames        = self.cows.index.astype(str)
        
        for i in self.cows.index:
            i = str(i) 
            start   = startcol.loc[i]
            stop    = stopcol.loc[i]
            date_range1 = pd.date_range(start=start, end=stop)
         
         
            lactx = self.milk.loc[date_range1, i]
            lactx = lactx.reset_index(drop=True)  # Reset the index to a standard numerical index
            
            if len(lactx) < maxrows:
                pad = maxrows - len(lactx)
                lactx = pd.concat(([lactx, pd.Series([np.nan] * pad)]))
                
            elif len(lactx) >= maxrows:
                lactx = lactx.iloc[:maxrows]
            
            
            lactx_list.append(lactx.tolist())  # Convert the Series to a list

        lact_np1 = np.array(lactx_list)
        lact_np     = np.transpose(lact_np1)
        lact_df = pd.DataFrame(lact_np, columns=colnames)
        lact_df.index.name = 'weeks'
        # lact_df = lact_df1.T
        return lact_np, lact_df, colnames
    
    def create_weekly(self):
        grouping_key = (self.lact_df.index // 7)
        lactwk = self.lact_df.groupby(grouping_key).mean()
        
        # lactwk = lactwk.reset_index()
        return lactwk
         
         
         
         
         
         
         
         
         
        #     lactx = self.milk.loc[date_range1,i]
        #     lact.append(lactx)
        
        # lact23 = pd.DataFrame(lact)
        # return lact23
    
    def write_to_csv(self):
        self.lact_df.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_daily.csv')
        self.lactwk .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_wk.csv')
                
                
            


# death_date         datetime64[ns]
# last calf#                float64
# last calf bdate    datetime64[ns]
# last stop date     datetime64[ns]
# dtype: object
           
            
           
        
        
    
# x = Lactations()

