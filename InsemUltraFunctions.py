'''InsemUltra.py'''
    
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from InsemUltraBasics import InsemUltraBasics

    
class InsemUltraFunctions:
    def __init__(self):
        
        self.IUB = InsemUltraBasics()
        
        self.lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates= ['b_date'], index_col=None) 
        self.bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], index_col='WY_id')

        self.u   = pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv',      parse_dates= ['ultra_date'])
        self.i   = pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv',      parse_dates= ['insem_date']) 
        
        self.today   = np.datetime64('today','D')
        self.rng     = list(range(1, self.bd1.index.max()+1))
        
        self.last_stop  = self.create_last_stop()
        self.last_insem = self.create_last_insem()
        self.last_ultra = self.create_last_ultra()
        
        self.last_ultra_insem = self.create_merge_last_ultra_insem()
  
        self.valid_insem_df  = self.create_valid_insem_df()
        self.insem_df        = self.create_insem_df()
        
        self.valid_ultra_df  = self.create_valid_ultra_df()
        self.ultra_df        = self.create_ultra_df()
        
        self.df2            = self.merge_insem_ultra()
        self.df3 = self.create_expected_bdate()
        
        self.ipiv           = self.create_last_insem_pivot()
        self.df4            = self.create_last_stop_merge()
        self.df5            = self.create_last_calf_merge()
        
    def create_last_stop(self):
        self.last_stop = self.IUB.last_stop
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
        return self.last_insem
    
        
    def create_last_ultra(self):
        
        u1 = self.u.groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max'
        })
        
        u1a = u1.merge( right=self.u[['WY_id','ultra_date', 'readex']], 
            on      =['WY_id', 'ultra_date'], 
            how     ='left'
            )
    
        u1a.rename(columns = {'Calf_num'  :'u_calf#',
                            'ultra_date':'u_date',
                            'Try_num'   : 'try#',
                            'readex'    : 'u_read'
                            },inplace=True)
        
        self.last_ultra = u1a.set_index("WY_id").reindex(self.rng)
        # print("last_ultra", self.last_ultra.loc[self.last_ultra.index > 260])
  
        return self.last_ultra
    
    
    def create_merge_last_ultra_insem(self):
        
        self.last_insem['age insem'] =  (self.today - self.last_insem['i_date']).dt.days
        self.last_ultra['age ultra'] =  (self.today - self.last_ultra['u_date']).dt.days
        
                                    # keep values from both df's with 'outer'          
        self.last_ultra_insem = self.last_insem.merge(self.last_ultra, 
                on      ='WY_id', 
                how     ='left', 
                suffixes=('_insem', '_ultra'))
        # last_ultra_insem.set_index('WY_id', inplace=True)
        return self.last_ultra_insem
    


    def create_valid_insem_df(self):
        
        valid_insem_mask1   =  (self.last_insem['i_calf#'] > self.IUB.last_calf['last calf#'])
        valid_insem_mask    =  self.last_insem['i_date'][valid_insem_mask1].to_frame()
        
        valid_insem_mask.reset_index(drop=True, inplace=True)

        valid_insem_df1 = self.last_insem[valid_insem_mask1].copy()
        self.valid_insem_df = valid_insem_df1.reindex(self.rng)
        
        # print("valid_insem_df ", self.valid_insem_df.loc[self.valid_insem_df.index > 260])       
        return self.valid_insem_df



                                    # combines all insem: last, valid, and age 
    def create_insem_df(self):
        
        age_insem1 = self.valid_insem_df.loc[(self.valid_insem_df['i_date'].notnull())].copy()
        age_insem1['age insem'] =  (self.today - age_insem1['i_date']).dt.days
        
        self.insem_df = self.valid_insem_df.merge(
            right   =age_insem1, 
            on      ='WY_id', 
            how     ='left', 
            suffixes=('', "_right"))
        
        self.insem_df.drop([i for i in self.insem_df.columns if '_right' in i], axis=1, inplace=True)

        # print("insem_df ", self.insem_df.loc[self.insem_df.index > 260])
        return self.insem_df
     

    
    def create_valid_ultra_df(self):
        valid_ultra_mask = ((self.last_ultra['u_date'] > self.valid_insem_df['i_date']) |
                            (self.valid_insem_df['i_date'].notna())
                            )  # for all cows incl dead
        
        
        valid_ultra_mask_df1 = self.last_ultra[['u_calf#','u_date', 'u_read']][valid_ultra_mask]                  # filters and attaches date
        # valid_ultra_mask_df1.index +=1 
        
        self.valid_ultra_df = valid_ultra_mask_df1.reindex(self.rng)
        self.valid_ultra_df.index.name = "WY_id"
        # print("valid_ultra_mask ", valid_ultra_mask.loc[valid_ultra_mask.index > 260])
        # print("valid_ultra_mask_df1 ", valid_ultra_mask_df1.loc[valid_ultra_mask_df1.index > 260])

        # print("valid_ultra_df ", self.valid_ultra_df.loc[self.valid_ultra_df.index > 260])

        
        return self.valid_ultra_df
    
    
    
                                        # combines all ultra:  last, valid and age
    def create_ultra_df(self)    :
        age_ultra1 = self.valid_ultra_df.loc[(self.valid_ultra_df['u_date'].notnull())].copy()
        age_ultra1['age ultra'] =  (self.today - age_ultra1['u_date']).dt.days
        age_ultra1.index.name = 'WY_id'
        
        self.ultra_df = self.valid_ultra_df.merge(
            right=age_ultra1, 
            on='WY_id', 
            how='left', 
            suffixes=('', "_right"))
        
        self.ultra_df.drop([i for i in self.ultra_df.columns if '_right' in i], axis=1, inplace=True)

        return self.ultra_df
    
    
    def merge_insem_ultra(self):
 

        self.df2 =pd.merge( self.insem_df, self.ultra_df, 
                    on       ='WY_id', 
                    how      ='left', 
                    suffixes =('', "_right"))
     
        # print("df2", self.last_ultra.tail(10))

        return self.df2
    
    
    def create_expected_bdate(self):
        
        bdemask =  (
            (self.df2['u_date'].notnull())  & 
            (self.df2['u_read'] == 'ok')    
            )
                            
        
        bde1 = self.df2[bdemask].copy()
        
        bde1['expected bdate'] = bde1['i_date'] + pd.to_timedelta(282, unit='D')
        bde = bde1['expected bdate']
    
        self.df3  = self.df2.merge(right=bde, on='WY_id', how='left' )
        self.df3.fillna(np.nan, inplace=True)
        
        # self.date_cols = ['last stop date','last calf bdate','i_date','u_date','expected bdate']
        
        return self.df3
    
    
    
    
    def create_last_insem_pivot(self):
        i2 = self.i.drop(columns=['typex', 'readex'])
        maxcalf1a = i2.loc[i2.groupby("WY_id")['calf_num'].idxmax()]
        maxcalf1 = maxcalf1a.loc[maxcalf1a.groupby("WY_id")['try_num'].idxmax()]
        
        maxcalf1 = maxcalf1.reset_index(drop=True)
        maxcalf2 = i2.merge(maxcalf1, 
                            how='right', 
                            on=['WY_id','calf_num','try_num'])
           
        maxcalf3 = maxcalf2.drop(columns=['insem_date_x'])
        
        self.ipiv = pd.pivot_table(maxcalf3, 
                    index   = ['WY_id', 'calf_num' ], 
                    columns = 'try_num' ,
                    values  = 'insem_date_y'  ,
                    aggfunc = 'max'
                    )
        return self.ipiv



    def create_last_stop_merge(self):
        self.df4 = self.df3.merge(self.last_stop,
                    how='left',
                    on='WY_id'
                    )
        
        return self.df4
    
    def create_last_calf_merge(self):
        self.df5 = self.df4.merge(self.IUB.last_calf,
                                  how='left',
                                  on="WY_id"
                                  )
        return self.df5