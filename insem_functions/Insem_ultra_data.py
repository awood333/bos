'''insem_ultra.py'''

import pandas as pd
import numpy as np
from datetime import datetime

from milk_functions.statusData2    import StatusData2
from MilkBasics                     import MilkBasics
from insem_functions.insem_ultra_basics import InsemUltraBasics



class InsemUltraData:
    def __init__(self):
        self.IUB = InsemUltraBasics()
        self.SD = StatusData2()
        self.status_col = self.SD.status_col
        
        self.mb = MilkBasics()
        
        self.data = self.mb.dataLoader()

        self.alive_mask = self.data['bd']['death_date'].isnull()     

        self.date_format = '%m/%d/%Y'
        self.today   = pd.Timestamp(datetime.today())

        self.last_insem         = self.create_last_insem()
        self.last_valid_insem   = self.create_last_valid_insem()
        self.last_invalid_insem = self.create_last_invalid_insem()
        
        self.last_ultra         = self.create_last_ultra()
        self.last_valid_ultra        = self.create_last_valid_ultra()
        self.last_invalid_ultra        = self.create_last_invalid_ultra()
        
        self.df7                = self.create_df()
        
        self.allx, self.all_milking, self.all_dry      = self.create_allx()
        
        self.create_write_to_csv()
        self.IUD_dash_vars = self.get_dash_vars()
        
        
    def create_last_insem(self):
        
        # get last insem event from 'i'
        i1 = self.data['i'].groupby('WY_id').last().reset_index()
        i2 = i1.rename(columns = {
            'calf_num'    : 'i_calf#',
            'insem_date'   : 'i_date',
            'try_num'      : 'try#'
            })
        
        #    merge last insem series with last calf series to set up comp below
        self.last_insem = i2.merge(self.IUB.last_calf,
                      on='WY_id',
                      how = 'left')

        return    self.last_insem        

    def create_last_valid_insem(self):
        
        i4 = self.last_insem.loc[(
            self.last_insem['i_calf#'] > self.last_insem['last calf#']
        )].reset_index(drop=True)
  
        self.last_valid_insem = i4[['WY_id','i_calf#', 'i_date' ,'last calf#']]
        
        return self.last_valid_insem
    
    
    
    def create_last_invalid_insem(self):
        
        i4 = self.last_insem.loc[(
            self.last_insem['i_calf#'] == self.last_insem['last calf#']
        )].reset_index(drop=True)
  
        self.last_invalid_insem = i4[['WY_id','i_calf#', 'i_date' ,'last calf#']]
        
        return self.last_invalid_insem



    def create_last_ultra(self):
        
        u1 = self.data['u'].groupby('WY_id').last().reset_index()
        self.last_ultra = u1.rename(columns = {'calf_num'  :'u_calf#',
                            'ultra_date':'u_date',
                            'readex'    : 'u_read'
                            })
        
        return self.last_ultra
    
    
    def create_last_valid_ultra(self):
        
        df = self.last_valid_insem.merge(self.last_ultra,
                            on = "WY_id",
                            how = 'left'
                            )
        
        df1 = df.loc[(
            df['u_date'] > df['i_date']
            )
                          ].reset_index(drop=True)
        
        df2 = df1.drop(columns=(['i_calf#','i_date','try_num', 'typex']))
               
        bdemask1 =  df2.loc[
            (df2 ['u_date'].notnull())  & 
            ((df2 ['u_read'] == 'ok' ) | (df2 ['u_read'] == 'x'))
            ]   
        
        bdemask = bdemask1 ['WY_id'].to_list()
                            
        valid_ultra1 = df2 [df2 ['WY_id'].isin(bdemask)].reset_index(drop=True)
        
        valid_ultra2 = valid_ultra1.merge(df[['WY_id', 'i_date']],
                             how = 'left',
                             on = 'WY_id'
                             )
# use .loc to set the 'if' conditions
        valid_ultra2.loc[
                valid_ultra2['u_read'] == ( 'ok'),'expected bdate'
                ] = valid_ultra2['i_date'] + pd.to_timedelta(282, unit='D')
            
        valid_ultra2.loc[
                valid_ultra2['u_read'] == ( 'x'),'expected bdate'
                ] = ''

   
        self.last_valid_ultra = valid_ultra2.drop(columns =  ['last calf#','i_date'])
        
        return self.last_valid_ultra
        
        
    def create_last_invalid_ultra(self):
        
        df = self.last_valid_insem.merge(self.last_ultra,
                            on = "WY_id",
                            how = 'left'
                            )
        
        df1 = df.loc[(
            df['u_date'] < df['i_date']     #note!  this is the Invalid ultra
            )
                          ].reset_index(drop=True)
        
        self.last_invalid_ultra = df1.drop(columns=(['i_calf#','i_date','try_num', 'typex']))     
           
        return self.last_invalid_ultra        
        
        
    def create_df(self):
        
        #merge the valid and invalid dfs
        df3  = self.last_valid_insem .merge( right = self.last_valid_ultra,
                              how='left',
                              on='WY_id' )        
        
        last_calf_cols = self.IUB.last_calf[['WY_id','last calf bdate', 'last calf age']]
        
        df3a = df3 . merge( right = last_calf_cols,
                           how = 'outer',
                           on = 'WY_id'
                           )
        

        last_stop_cols = self.IUB.last_stop[['WY_id','stop calf#','last stop date']]
        
        df4 =       df3a.merge(right=last_stop_cols ,
                              on='WY_id', 
                              how='left' )  
        
        # df4               .to_csv('F:\\COWS\\data\\insem_data\\df4.csv') 

        df4['age insem'] =  (self.today - df4['i_date']).dt.days
        df4['age ultra'] =  (self.today - df4['u_date']).dt.days
        df4['i_check']    = df4['i_calf#'] - df4['last calf#']

        df4['u_check1']  =  df4['u_calf#'] - df4['last calf#'] 
        df4['u_check2']  = (df4['u_date'] - df4['i_date']).dt.days
        
        df5 = df4.rename(columns={'last calf age' : 'days milking'})
        
        df6 = df5.merge(right=self.status_col[['ids','status']], 
                             left_on= 'WY_id', right_on ='ids',
                             suffixes=('', 'R'))
   

        df6         = df6[df6['status'] != 'G']
        df6         = df6.drop(columns=['ids'])
        df6         = df6.reset_index(drop=True)

        self.df7 = df6
        
        return self.df7
    
    
    def create_allx(self):
        
        self.allx = self.df7[
            [
            'WY_id',
            'status',
            'last stop date',
            'stop calf#',
            'last calf bdate',
            'last calf#',
            'days milking',
            'i_calf#',
            'i_date',
            'age insem',
            'u_calf#',
            'u_date',
            'u_read',
            'age ultra',
            'expected bdate',
            'i_check',
            'u_check1',
            'u_check2'
            ]
        ]
        
        self.all_milking = self.allx[self.allx['status'] == 'M']
        self.all_dry     = self.allx[self.allx['status'] == 'D']
        
        return self.allx, self.all_milking, self.all_dry
    
    
    def create_write_to_csv(self):
              
        self.allx        .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
        self.all_milking .to_csv('F:\\COWS\\data\\insem_data\\all_milking.csv')
        self.all_dry     .to_csv('F:\\COWS\\data\\insem_data\\all_dry.csv')
        self.last_valid_ultra  .to_csv('F:\\COWS\\data\\insem_data\\last_valid_ultra.csv') 
    
    
    def get_dash_vars(self):
        self.IUD_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.IUD_dash_vars  
        
        
if __name__ == "__main__":
    iud = InsemUltraData()
    IUD_dash_vars = iud.get_dash_vars()
