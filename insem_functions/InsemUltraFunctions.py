'''InsemUltra.py'''
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
import pandas as pd
import numpy as np

from insem_functions.InsemUltraBasics import InsemUltraBasics
from milk_functions.status_ids import StatusData
from milk_functions.status_2     import StatusData2

today   = np.datetime64('today','D')

class InsemUltraFunctions:
    def __init__(self):
        
        self.IUB = InsemUltraBasics()
        self.SD  = StatusData()
        self.alive_ids = self.SD.alive_ids[-1]
        self.SD2 = StatusData2()


        
        self.data       = self.read_data()
        self.last_stop  = self.create_last_stop()
        self.last_insem = self.create_last_insem()
        self.last_ultra = self.create_last_ultra()
        
        self.last_ultra_insem   = self.create_merge_last_ultra_insem()
        self.valid_insem_df     = self.create_valid_insem_df()
        self.insem_df           = self.create_insem_df()
        
        self.valid_ultra_df     = self.create_valid_ultra_df()
        self.ultra_df           = self.create_ultra_df()
        self.df2                = self.merge_insem_ultra()
        self.df3                = self.create_expected_bdate()
        

        self.df4            = self.create_last_stop_merge()
        self.df5            = self.create_last_calf_merge()
        self.allx           = self.create_allx()
        
        self.ipiv_alive     = self.create_last_insem_pivot()
        self.upiv_alive     = self.create_last_ultra_pivot()
        self.write_to_csv()
        self.IUF_dash_vars  = self.get_dash_vars()


    def read_data(self):
        
        lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', index_col=None) 
        bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', index_col='WY_id')
        u   = pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv' )
        i   = pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv' ) 
        
        lb['b_date'] = pd.to_datetime(lb['b_date'])
        bd1['birth_date'] = pd.to_datetime(bd1['birth_date'])
        bd1['death_date'] = pd.to_datetime(bd1['death_date'])
        bd1['adj_bdate'] = pd.to_datetime(bd1['adj_bdate'])
        u['ultra_date'] = pd.to_datetime(u['ultra_date'])
        i['insem_date'] = pd.to_datetime(i['insem_date'])
        
        rng     = list(range(1, bd1.index.max()+1))
        
        self.data = {
        'lb'    : lb,
        'bd1'   : bd1,
        'u'     : u,
        'i'     : i,
        'rng'   : rng
    }
        return self.data
        
                
             
    def create_last_stop(self):
        self.last_stop = self.IUB.last_stop
        return self.last_stop
            
    def create_last_insem(self):
        
        i1 = self.data['i'].groupby('WY_id',as_index=True).agg({
        'calf_num':'max',
        'insem_date':'max'        
        })
        
        i1.rename(columns = {'calf_num'     :'i_calf#',
                            'insem_date'   :'i_date',
                            'try_num'      : 'try#'
                            },inplace=True)
        
        self.last_insem = i1.reindex(self.data['rng'])
        return self.last_insem
    
    def create_last_ultra(self):
        
        u1 = self.data['u'].groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max'
        })
        
        u1a = u1.merge( right=self.data['u'][['WY_id','ultra_date', 'readex']], 
            on      =['WY_id', 'ultra_date'], 
            how     ='left'
            )
    
        u1a.rename(columns = {'Calf_num'  :'u_calf#',
                            'ultra_date':'u_date',
                            'Try_num'   : 'try#',
                            'readex'    : 'u_read'
                            },inplace=True)
        
        self.last_ultra = u1a.set_index("WY_id").reindex(self.data['rng'])
  
        return self.last_ultra
    
    def create_merge_last_ultra_insem(self):
        
        self.last_insem['age insem'] =  (today - self.last_insem['i_date']).dt.days
        self.last_ultra['age ultra'] =  (today - self.last_ultra['u_date']).dt.days
        
                                    # keep values from both df's with 'outer'          
        self.last_ultra_insem = self.last_insem.merge(self.last_ultra, 
                on      ='WY_id', 
                how     ='left', 
                suffixes=('_insem', '_ultra'))

        return self.last_ultra_insem


    def create_valid_insem_df(self):
        
        valid_insem_mask1   =  (self.last_insem['i_calf#'] > self.IUB.last_calf['last calf#'])
        valid_insem_mask    =  self.last_insem['i_date'][valid_insem_mask1].to_frame()
        
        valid_insem_mask.reset_index(drop=True, inplace=True)

        valid_insem_df1 = self.last_insem[valid_insem_mask1].copy()
        self.valid_insem_df = valid_insem_df1.reindex(self.data['rng'])
     
        return self.valid_insem_df


    # combines all insem: last, valid, and age 
    def create_insem_df(self):
        
        age_insem1 = self.valid_insem_df.loc[(self.valid_insem_df['i_date'].notnull())].copy()
        age_insem1['age insem'] =  (today - age_insem1['i_date']).dt.days
        
        self.insem_df = self.valid_insem_df.merge(
            right   =age_insem1, 
            on      ='WY_id', 
            how     ='left', 
            suffixes=('', "_right"))
        
        self.insem_df.drop([i for i in self.insem_df.columns if '_right' in i], axis=1, inplace=True)

        return self.insem_df
     

    def create_valid_ultra_df(self):
        
        valid_ultra_mask = ((self.last_ultra['u_date'] > self.valid_insem_df['i_date']) |
                            (self.valid_insem_df['i_date'].notna())
                            )  # for all cows incl dead
        
        valid_ultra_mask_df1 = self.last_ultra[['u_calf#','u_date', 'u_read']][valid_ultra_mask]                  # filters and attaches date
        
        self.valid_ultra_df = valid_ultra_mask_df1.reindex(self.data['rng'])
        self.valid_ultra_df.index.name = "WY_id"

        return self.valid_ultra_df
    

    # combines all ultra:  last, valid and age
    def create_ultra_df(self)    :
        age_ultra1 = self.valid_ultra_df.loc[(self.valid_ultra_df['u_date'].notnull())].copy()
        age_ultra1['age ultra'] =  (today - age_ultra1['u_date']).dt.days
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
    
    def create_allx(self):
        
        self.df5_reset = self.df5.reset_index()
        df6 = self.df5_reset.merge(self.SD2.status_col[['ids','status']], 
                                       how='left', 
                                       left_on='WY_id', 
                                       right_on='ids', 
                                       suffixes=('', '_right'))
        
        # df6.drop(columns=['ids'], inplace=True)
        df6.set_index('WY_id', inplace=True)

        df7 = df6.loc[df6['status'] != 'G']
        new_col_order =  [
            'status',
            'last stop date',
            'stop calf#',
            'last calf bdate',
            'last calf#',
            'last calf age',
            # 'days milking',
            'i_calf#',
            'i_date',
            'age insem',
            'u_calf#',
            'u_date',
            'u_read',
            'age ultra',
            'expected bdate'
            ]
        df7 = df7[new_col_order]
        
        self.allx = df7
        
        return self.allx
    
    def create_last_insem_pivot(self):
        i2 = self.data['i'].drop(columns=['typex', 'readex'])
        
        maxcalf1 = i2.groupby('WY_id')['calf_num'].max()
        maxcalf1a = maxcalf1.reindex(self.data['rng'])
        maxcalf1a = maxcalf1a.astype('Int64')

        maxcalf2 = i2.merge(maxcalf1, 
                            how='right', 
                            on=['WY_id','calf_num']
                            )

        ipiv_allcows = pd.pivot_table(maxcalf2, 
                    index   = 'WY_id', 
                    columns = 'try_num' ,
                    values  = 'insem_date' 
                    )
        
        ipiv_allcows = ipiv_allcows.reindex(self.data['rng'])
        ipiv_allcows.insert(0,'lact#', maxcalf1a)
        self.ipiv_alive = ipiv_allcows.loc[self.alive_ids]
        
        return self.ipiv_alive
    
    
    def create_last_ultra_pivot(self):
        u2 = self.data['u'].drop(columns=['typex', 'readex'])
        
        maxcalf1 = u2.groupby('WY_id')['Calf_num'].max()
        maxcalf1a = maxcalf1.reindex(self.data['rng'])
        maxcalf1a = maxcalf1a.astype('Int64')

        maxcalf2 = u2.merge(maxcalf1, 
                            how='right', 
                            on=['WY_id','Calf_num']
                            )

        upiv_allcows = pd.pivot_table(maxcalf2, 
                    index   = 'WY_id', 
                    columns = 'Try_num' ,
                    values  = 'ultra_date' 
                    )
        
        upiv_allcows = upiv_allcows.reindex(self.data['rng'])
        upiv_allcows.insert(0,'lact#', maxcalf1a)
        
        self.upiv_alive = upiv_allcows.loc[self.alive_ids]
        
        return self.upiv_alive
    
    
    def write_to_csv(self):
    
        self.allx       .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
        self.ipiv_alive .to_csv ('F:\\COWS\\data\\insem_data\\ipiv.csv')
        self.upiv_alive .to_csv ('F:\\COWS\\data\\insem_data\\upiv.csv')
        
        
    def get_dash_vars(self):
        self.IUF_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.IUF_dash_vars
        
     
if __name__ == "__main__":
    iuf=InsemUltraFunctions()
    IUF_dash_vars = iuf.get_dash_vars()