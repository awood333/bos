import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, timedelta


class InsemUltraData:
    def __init__(self):

        self.lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates= ['b_date'], index_col=None) 
        self.bd1 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], index_col='WY_id')
        self.u   = pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv',      parse_dates= ['ultra_date'])
        self.i   = pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv',      parse_dates= ['insem_date']) 
        self.s   = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv', parse_dates= ['stop'])
        self.f   = pd.read_csv ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0,parse_dates= ['datex']) 
        
  

        self.today   = np.datetime64('today','D')
        self.rng     = list(range(1, self.bd1.index.max()+1))
        self.df      = pd.DataFrame()
        
        self.df['WY_id'] = self.rng
        self.df.set_index('WY_id',drop=True,inplace=True)
        self.date_format = '%m/%d/%Y'
        
        self.bd         = self.create_bd()
        self.first_calf = self.create_first_calf()
        self.last_calf  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()
        
        self.df         = self.create_df()
        self.last_insem = self.create_last_insem()
        self.last_ultra = self.create_last_ultra()
        
        self.last_ultra_insem = self.create_merge_last_ultra_insem()
  
        
        self.valid_insem_df  = self.create_valid_insem_df()
        self.insem_df        = self.create_insem_df()
        
        self.valid_ultra_df  = self.create_valid_ultra_df()
        self.ultra_df       = self.create_ultra_df()
        
        self.df2             = self.merge_insem_ultra()
        self.all1            = self.create_expected_bdate()
     
        
        self.create_write_to_csv()

        
        
                                # bd wy's will stay constant throughout - and stay as a col, not index
    def create_bd(self):
        
        bd = self.bd1[['adj_bdate', 'death_date']]
        bd.rename(columns={'adj_bdate': 'b_date'})
        return bd

    def create_first_calf(self):
        
        first_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'min',
            'calf#'   : 'min'
            }).reset_index()
        
        first_calf1.rename(columns={'calf#': 'first calf#',
            'b_date': 'first calf bdate'}, inplace=True)
        
        first_calf = self.bd.merge(right=first_calf1, how='left', 
            left_index=True, right_on='WY_id', suffixes=('', "_right") )

        first_calf.drop([i for i in first_calf.columns if '_right' in i], axis=1, inplace=True)
        
        return first_calf
    

    def create_last_calf(self):
        
        last_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        
        last_calf1.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'}, inplace=True)
        
                                    # keep wy col intact 
        last_calf = self.bd.merge(right=last_calf1,   how='left', 
            left_index=True, right_on='WY_id', suffixes=('', "_right") )
        
        last_calf.drop([i for i in last_calf.columns if '_right' in i], axis=1, inplace=True)
        
        last_calf.set_index('WY_id', inplace=True)
        return last_calf
    


    def create_last_stop(self):
        last_stop1 = self.s.groupby('WY_id').agg({
            'lact_num'    : 'max',
            'stop'        : 'max'
        })   
        last_stop1.rename(columns = {'lact_num':'stop calf#',
            'stop':'last stop date'
            },inplace=True)

                                    # keep wy col intact with 'left'        
        last_stop = self.bd.merge(right=last_stop1,  how='left', 
            left_index=True, right_on='WY_id', suffixes=('', "_right"))
        
        last_stop.drop([i for i in last_stop.columns if '_right' in i], axis=1, inplace=True)
        
        last_stop.reset_index(drop=True, inplace=True)
        last_stop['last stop date'].fillna(np.nan, inplace=True)
        return last_stop
    

    
                                    #merge bd and last calf with last stop
    def create_df(self):
        
        df_1 = self.bd.merge(right= self.last_stop,  how='left', 
            left_index=True, right_on='WY_id', suffixes=('', "_right"))
        
        df_1.drop([i for i in df_1.columns if '_right' in i], axis=1, inplace=True)
        
                                    # keep values from both df's with 'outer'        
        df = df_1.merge(right=self.last_calf, on="WY_id", how='outer', 
            suffixes=('', "_right"))
        
        df.drop([i for i in df.columns if '_right' in i], axis=1, inplace=True)
        df.reset_index( drop=True, inplace=True)
        return df




    def create_last_insem(self):
        
        i1 = self.i.groupby('WY_id',as_index=True).agg({
        'calf_num':'max',
        'insem_date':'max'        
        })
        
        i1.rename(columns = {'calf_num'     :'i_calf#',
                            'insem_date'   :'i_date',
                            'try_num'      : 'try#'
                            },inplace=True)
        
        last_insem = self.bd.merge(right=i1,  how='left', 
            left_index=True, right_on='WY_id', suffixes=('', "_right") )
        
        last_insem.drop([i for i in last_insem.columns if '_right' in i], axis=1, inplace=True)
        
        last_insem.reset_index(drop=True, inplace=True)
        last_insem['i_date'].fillna(np.nan, inplace=True)
        return last_insem
    
        
    def create_last_ultra(self):
        
        u1 = self.u.groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max'
        # 'readex'        : 'first'  
        })
        
        u1a = u1.merge( right=self.u[['WY_id','ultra_date', 'readex']], 
            on=['WY_id', 'ultra_date'], how='left', suffixes=('', "_right"))
        
        u1a.drop([i for i in u1.columns if '_right' in i], axis=1, inplace=True)
        
        u1a.rename(columns = {'Calf_num'  :'u_calf#',
                            'ultra_date':'u_date',
                            'Try_num'   : 'try#',
                            'readex'    : 'u_read'
                            },inplace=True)
        
        last_ultra = self.bd.merge(right=u1a, how='left', 
            left_index=True, right_on='WY_id', suffixes=('', "_right") )
        
        last_ultra.drop([i for i in last_ultra.columns if '_right' in i], axis=1, inplace=True)
        
        last_ultra.reset_index(drop=True, inplace=True)
        last_ultra['u_date'].fillna(np.nan, inplace=True)
        return last_ultra
    
    
    def create_merge_last_ultra_insem(self):
        
        self.last_insem['age insem'] =  (self.today - self.last_insem['i_date']).dt.days
        self.last_ultra['age ultra'] =  (self.today - self.last_ultra['u_date']).dt.days
        
                                    # keep values from both df's with 'outer'          
        last_ultra_insem = self.last_insem.merge(self.last_ultra, on='WY_id', how='outer', 
            suffixes=('_insem', '_ultra'))
        return last_ultra_insem
    


    def create_valid_insem_df(self):
        
        valid_insem_mask1   =  (self.last_insem['i_calf#'] > self.df['last calf#']) 
        valid_insem_mask    =  self.last_insem['i_date'][valid_insem_mask1].to_frame()
        
        valid_insem_mask.reset_index(drop=True, inplace=True)
        valid_insem_df1 = self.last_insem[valid_insem_mask1].copy()
        
        valid_insem_df = self.bd.merge(right=valid_insem_df1, how='left', 
                            left_index=True, right_on='WY_id', suffixes=('', "_right")  )
        
        valid_insem_df.drop([i for i in valid_insem_df.columns if '_right' in i], axis=1, inplace=True)
        
        valid_insem_df.reset_index(drop=True, inplace=True)
        return valid_insem_df


                                    # combines all insem: last, valid, and age 
    def create_insem_df(self):
          
        age_insem1 = self.valid_insem_df.loc[(self.valid_insem_df['i_date'].notnull())].copy()
        age_insem1['age insem'] =  (self.today - age_insem1['i_date']).dt.days
        
        insem_df = self.valid_insem_df.merge(right=age_insem1, on='WY_id', how='left', 
            suffixes=('', "_right"))
        
        insem_df.drop([i for i in insem_df.columns if '_right' in i], axis=1, inplace=True)
        return insem_df


       
    def create_valid_ultra_df(self):
        valid_ultra_mask = ((self.last_ultra['u_date'] > self.valid_insem_df['i_date']) &
                            ((self.valid_insem_df['i_date'].notnull())) | 
                            (self.valid_insem_df['i_date'].notna()) )   # for all cows incl dead
        
        valid_ultra_mask_df1 = self.last_ultra[['u_date', 'u_read']][valid_ultra_mask]                  # filters and attaches date
        valid_ultra_mask_df1.index +=1 
        
        valid_ultra_mask_df = self.bd.merge(valid_ultra_mask_df1, left_index=True, right_index=True, 
                                how='left', suffixes=('_left', "__right") )
        
        valid_ultra_df   = self.last_ultra.merge(right=valid_ultra_mask_df, on='WY_id', 
                                how='left', suffixes=('_left', ""))
        
        valid_ultra_df.reset_index(drop=True, inplace=True)
        
        valid_ultra_df.drop([i for i in valid_ultra_df.columns if '_left' in i], axis=1, inplace=True)
        return valid_ultra_df
    
    
    
                                        # combines all ultra:  last, valid and age
    def create_ultra_df(self)    :
        age_ultra1 = self.valid_ultra_df.loc[(self.valid_ultra_df['u_date'].notnull())].copy()
        age_ultra1['age ultra'] =  (self.today - age_ultra1['u_date']).dt.days
        
        ultra_df = self.valid_ultra_df.merge(right=age_ultra1, on='WY_id', how='left', 
            suffixes=('', "_right"))
        
        ultra_df.drop([i for i in ultra_df.columns if '_right' in i], axis=1, inplace=True)
        return ultra_df
    
    
                                        #merge insem_df with ultra_df and with df
    def merge_insem_ultra(self):
        insem_cols = list(self.insem_df.columns)
        ultra_cols = list(self.ultra_df.columns)

        df2a =pd.merge( self.insem_df, self.ultra_df, on='WY_id', how='outer', suffixes=('', "_right"))
        
        df2a.drop([i for i in df2a.columns if '_right' in i], axis=1, inplace=True)
        
        df2 = pd.merge(self.df, df2a, on='WY_id',  suffixes=('', "_right") )
        df2.drop([i for i in df2.columns if '_right' in i], axis=1, inplace=True)
        
        return df2
    
    
                                        # expected bdate and merge with status_col from status module
    def create_expected_bdate(self):
        
        bdemask =  (
            (self.df2['u_date'].notnull())  & 
            (self.df2['u_read'] == 'ok')    &
            (self.df2['death_date'].isnull())
            )
                            
        
        bde1 = self.df2[bdemask].copy()
        
        bde1['expected bdate'] = bde1['i_date'] + pd.to_timedelta(282, unit='D')
        bde = bde1[['WY_id', 'expected bdate']]
        bde.set_index('WY_id', inplace=True)
        
        
        df3  = self.df2.merge(right=bde, on='WY_id', how='left' )
        
        # df3.drop([i for i in df3.columns if '_left' in i], axis=1, inplace=True)
        df3.drop(columns= 'adj_bdate', inplace=True)
        df3.set_index('WY_id', inplace=True)
        all1 = df3
        return all1

           
    
    def create_write_to_csv(self):
        
        self.last_calf      .to_csv('F:\\COWS\\data\\insem_data\\lb_last.csv')
        self.bd             .to_csv('F:\\COWS\\data\\insem_data\\bd.csv')
  
        self.valid_ultra_df .to_csv('F:\\COWS\\data\\insem_data\\valid_ultra_df.csv')
        self.last_ultra     .to_csv('F:\\COWS\\data\\insem_data\\last_ultra.csv')
        self.ultra_df       .to_csv('F:\\COWS\\data\\insem_data\\ultra_df.csv')
        
        self.last_insem     .to_csv('F:\\COWS\\data\\insem_data\\last_insem.csv')       
        self.valid_insem_df .to_csv('F:\\COWS\\data\\insem_data\\valid_insem.csv')
        
        self.df2       .to_csv('F:\\COWS\\data\\insem_data\\df2.csv')
        
        
        self.last_ultra_insem.to_csv('F:\\COWS\\data\\insem_data\\last_ultra_insem.csv')
        self.all1           .to_csv('F:\\COWS\\data\\insem_data\\all.csv')

