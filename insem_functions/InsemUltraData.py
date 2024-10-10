'''insem_ultra.py'''

import pandas as pd
import numpy as np

from milk_functions.status_2 import StatusData2
from MilkBasics import MilkBasics
from insem_functions.InsemUltraBasics import InsemUltraBasics


today   = np.datetime64('today','D')

class InsemUltraData:
    def __init__(self):
        self.IUB = InsemUltraBasics()
        self.SD = StatusData2()
        self.status_col = self.SD.status_col
        
        self.mb = MilkBasics()
        
        self.data = self.mb.dataLoader()

        self.alive_mask = self.data['bd']['death_date'].isnull()     

        self.date_format = '%m/%d/%Y'
        
        self.last_insem = self.create_last_insem()
        self.last_ultra = self.create_last_ultra()
        
        self.valid_insem = self.create_valid_insem()
        self.valid_ultra = self.create_valid_ultra()
        
        self.df2            = self.merge_insem_ultra()
        self.df7                 = self.create_expected_bdate()
        
        self.allx           = self.create_allx()
        self.create_write_to_csv()
        self.IUD_dash_vars = self.get_dash_vars()
        
        
            

    def create_last_insem(self):
        
        i1 = self.data['i'].groupby('WY_id',as_index=True).agg({
        'calf_num':'max',
        'insem_date':'max'        
        }).reset_index()
        
        i1.rename(columns = {'calf_num'     :'i_calf#',
                            'insem_date'   :'i_date',
                            'try_num'      : 'try#'
                            },inplace=True)
        
        self.last_insem = i1.reindex(self.data['rng'])
        self.last_insem['i_date'] = self.last_insem['i_date'].fillna(np.nan)
        return self.last_insem
    
        
    def create_last_ultra(self):
        
        #u1 is a mask for max calf_num and max ultra date
        u1 = self.data['u'].groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max'
        # 'readex'        : 'first'  
        }).reset_index()
        
        #mask out all but the last ultra date and get the other fields
        u1a = u1.merge( right=self.data['u'][['WY_id','ultra_date', 'readex']], 
            on      =['WY_id', 'ultra_date'], 
            how     ='left', 
            suffixes=('', "_right"))
        
        u1a.drop([i for i in u1.columns if '_right' in i], axis=1, inplace=True)
        
        u1a.rename(columns = {'Calf_num'  :'u_calf#',
                            'ultra_date':'u_date',
                            'Try_num'   : 'try#',
                            'readex'    : 'u_read'
                            },inplace=True)
        
        self.last_ultra = u1a.reindex(self.data['rng'])

        return self.last_ultra
    
    
    def create_valid_insem(self):
        
        last_insem_last_calf = self.last_insem.merge(self.IUB.last_calf, 
                on      ='WY_id', 
                how     ='left', 
                suffixes=('_insem', '_ultra'))
 
        valid_insem_mask   =  (last_insem_last_calf['i_calf#'] > last_insem_last_calf['last calf#']) 
                
        valid_insem1 = last_insem_last_calf[valid_insem_mask]
        self.valid_insem = valid_insem1.reindex(self.data['rng'])

        return self.valid_insem
    


    def create_valid_ultra(self):
        
        valid_ultra_mask1    = (
                (self.last_ultra['u_date'] > self.valid_insem['i_date']) 
                )
        
        valid_ultra_mask = valid_ultra_mask1.reindex(self.data['rng'])
        valid_ultra1 = self.last_ultra[valid_ultra_mask]
        self.valid_ultra  = valid_ultra1.reindex(self.data['rng'])
        
        return self.valid_ultra
        
        
    def merge_insem_ultra(self):
 
        df1 = self.last_insem.merge(self.valid_ultra,
            on      ='WY_id', 
            how     ='left', 
            suffixes=('_left', '_calf')
        )
        
        df1['age insem'] =  (today - df1['i_date']).dt.days
        df1['age ultra'] =  (today - df1['u_date']).dt.days
        df1 = df1.sort_index()
        self.alive_mask.sort_index()
        # df1.index +=1
        
        self.df2 = df1[self.alive_mask]
        
        return self.df2


    
# expected bdate AND status_col merge
    def create_expected_bdate(self):
        
        
        bdemask =  (
            (self.df2['u_date'].notnull())  & 
            (self.df2['u_read'] == 'ok')   
            )
                            
        bde1 = self.df2[bdemask].copy()
        
        bde1['expected bdate'] = bde1['i_date'] + pd.to_timedelta(282, unit='D')
        bde = bde1['expected bdate']
    
    
        df3  = self.df2.merge(right=bde,
                              how='left',
                              left_on='WY_id',
                              right_index=True )
        
        df4 =       df3.merge(right=self.IUB.last_stop,
                              on='WY_id', how='left' )  #let = left outer
        
        df5 =       df4.merge(right=self.IUB.last_calf,
                              on='WY_id',
                              how='left' )

        lastcalf_age = [(today - date).days for date in df5['last calf bdate']]
        
        df5['days milking'] = lastcalf_age
        # df6 = df5.reindex(self.data['rng'])
        
        # self.status_col.index += 1
        
        df6 = df5.merge(right=self.status_col[['ids','status']], 
                             left_on= 'WY_id', right_on ='ids',
                             suffixes=('', '_right'))
   
        # df6 = df6.reset_index()

        df6 = df6[df6['status'] != 'G']
        df6 = df6.drop(columns=['ids'])
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
            'expected bdate'
            ]
        ]
        
        return self.allx
    
    
    def create_write_to_csv(self):
              
        self.allx               .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
        
    
    
    def get_dash_vars(self):
        self.IUD_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.IUD_dash_vars  
        
        
if __name__ == "__main__":
    iud = InsemUltraData()
    IUD_dash_vars = iud.get_dash_vars()
