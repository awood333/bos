'''insem_ultra.py'''

import pandas as pd
import numpy as np
import datetime
from datetime import date, timedelta

from status_2 import StatusData


class InsemUltraData:
    def __init__(self):

        self.lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates= ['b_date'], index_col=None) 
        self.bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], index_col='WY_id')
        self.u   = pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv',      parse_dates= ['ultra_date'])
        self.i   = pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv',      parse_dates= ['insem_date']) 
        self.s   = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv', parse_dates= ['stop'])
        self.f   = pd.read_csv ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0,parse_dates= ['datex']) 
        
        sd = StatusData()
        self.status_col = sd.status_col

        self.today   = np.datetime64('today','D')
        self.rng     = list(range(1, self.bd1.index.max()+1))
        self.df      = pd.DataFrame()
        
        self.death_mask = self.bd1['death_date'].notnull()
        self.alive_mask = self.bd1['death_date'].isnull()
        
        self.df['WY_id'] = self.rng
        self.df.set_index('WY_id',drop=True,inplace=True)
        self.date_format = '%m/%d/%Y'
        
        self.first_calf = self.create_first_calf()
        self.last_calf  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()
        
        self.last_insem = self.create_last_insem()
        self.last_ultra = self.create_last_ultra()
        
        self.valid_insem = self.create_valid_insem()
        self.valid_ultra = self.create_valid_ultra()
        
        self.df2            = self.merge_insem_ultra()
        self.df7                 = self.create_expected_bdate()
        
        self.allx           = self.create_allx()
        self.create_write_to_csv()


        
        
                                # bd wy's will stay constant throughout - and stay as a col, not index

    def create_first_calf(self):
        
        first_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'min',
            'calf#'   : 'min'
            }).reset_index()
        
        self.first_calf = first_calf1.set_index('WY_id').reindex(self.rng)
        self.first_calf.rename(columns={'calf#': 'first calf#',
            'b_date': 'first calf bdate'}, inplace=True)
        
        return self.first_calf
    

    def create_last_calf(self):
        
        last_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        
        self.last_calf = last_calf1.set_index("WY_id").reindex(self.rng)
        self.last_calf.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'}, inplace=True)

        return self.last_calf
    


    def create_last_stop(self):
        last_stop1 = self.s.groupby('WY_id').agg({
            'lact_num'    : 'max',
            'stop'        : 'max'
        })   
        
        self.last_stop = last_stop1.reindex(self.rng)
        self.last_stop.rename(columns = {'lact_num':'stop calf#','stop':'last stop date'
            },inplace=True)       
        
        self.last_stop['last stop date'].fillna(np.nan, inplace=True)
        return self.last_stop



    def create_last_insem(self):
        
        i1 = self.i.groupby('WY_id',as_index=True).agg({
        'calf_num':'max',
        'insem_date':'max'        
        })
        
        i1.rename(columns = {'calf_num'     :'i_calf#',
                            'insem_date'   :'i_date',
                            'try_num'      : 'try#'
                            },inplace=True)
        
        self.last_insem = i1.reindex(self.rng)
        self.last_insem['i_date'].fillna(np.nan, inplace=True)
        return self.last_insem
    
        
    def create_last_ultra(self):
        
        #u1 is a mask for max calf_num and max ultra date
        u1 = self.u.groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max'
        # 'readex'        : 'first'  
        })
        
        #mask out all but the last ultra date and get the other fields
        u1a = u1.merge( right=self.u[['WY_id','ultra_date', 'readex']], 
            on      =['WY_id', 'ultra_date'], 
            how     ='left', 
            suffixes=('', "_right"))
        
        u1a.drop([i for i in u1.columns if '_right' in i], axis=1, inplace=True)
        
        u1a.rename(columns = {'Calf_num'  :'u_calf#',
                            'ultra_date':'u_date',
                            'Try_num'   : 'try#',
                            'readex'    : 'u_read'
                            },inplace=True)
        
        self.last_ultra = u1a.set_index("WY_id").reindex(self.rng)
        # self.last_ultra['u_date'].fillna(np.nan, inplace=True)

        return self.last_ultra
    
    
    def create_valid_insem(self):
        
        last_insem_last_calf = self.last_insem.merge(self.last_calf, 
                on      ='WY_id', 
                how     ='left', 
                suffixes=('_insem', '_ultra'))
 
        valid_insem_mask   =  (last_insem_last_calf['i_calf#'] 
                               > last_insem_last_calf['last calf#']) 
                
        valid_insem1 = last_insem_last_calf[valid_insem_mask]
        self.valid_insem = valid_insem1.reindex(self.rng)

        return self.valid_insem
    


    def create_valid_ultra(self):
        
        valid_ultra_mask1    = (
                (self.last_ultra['u_date'] > self.valid_insem['i_date']) 
                # | self.last_ultra['u_date'].isnull()
                )
        
        valid_ultra_mask = valid_ultra_mask1.reindex(self.rng)
        print('True vals in valid_ultra_mask', sum(valid_ultra_mask))
        
        valid_ultra1 = self.last_ultra[valid_ultra_mask]
        self.valid_ultra  = valid_ultra1.reindex(self.rng)
        print('non-NaN vals in u-date', self.valid_ultra['u_date'].count())
        
        
        return self.valid_ultra
        
        
    def merge_insem_ultra(self):
 
        df1 = self.last_insem.merge(self.valid_ultra,
            on      ='WY_id', 
            how     ='left', 
            suffixes=('_left', '_calf')
        )
        
        df1['age insem'] =  (self.today - df1['i_date']).dt.days
        df1['age ultra'] =  (self.today - df1['u_date']).dt.days
        print('non-NaN vals in age ultra', df1['age ultra'].count())
        
        self.df2 = df1[self.alive_mask]
        
        return self.df2


    
# expected bdate 
    def create_expected_bdate(self):
        
        
        bdemask =  (
            (self.df2['u_date'].notnull())  & 
            (self.df2['u_read'] == 'ok')   
            )
                            
        
        bde1 = self.df2[bdemask].copy()
        
        bde1['expected bdate'] = bde1['i_date'] + pd.to_timedelta(282, unit='D')
        bde = bde1['expected bdate']
    
        df3  = self.df2.merge(right=bde,            on='WY_id', how='left' )
        df4 =       df3.merge(right=self.last_stop, on='WY_id', how='left' )
        df5 =       df4.merge(right=self.last_calf, on='WY_id', how='left' )

        lastcalf_age = [(self.today - date).days for date in df5['last calf bdate']]
        
        df5['days milking'] = lastcalf_age
        df6 = df5.reindex(self.rng)
        
        df7a = df6.merge(right=self.status_col[['ids','status']], 
                             left_on='WY_id', right_on='ids',
                             suffixes=('', '_right'))
   
        df7a.index.name = 'WY_id'     
        df7b = df7a.reindex(self.rng)
        
   

        
        
        df7b.drop(columns=['ids'], inplace=True)
        
        self.df7 = df7b[df7b['status'] != 'G']
        
        # self.date_cols = ['death_date','last stop date','last calf bdate','i_date','u_date','expected bdate']
        print('df7  ',self.df7.iloc[:5,:])
       
        return self.df7
    
    
    def create_allx(self):

        self.allx = self.df7[
            [
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
        print(self.df7)
        return self.allx
    
    
    def create_write_to_csv(self):
        
        self.last_calf      .to_csv('F:\\COWS\\data\\insem_data\\lb_last.csv')
        self.last_ultra     .to_csv('F:\\COWS\\data\\insem_data\\last_ultra.csv')
        
        self.allx               .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
