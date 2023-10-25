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
  
        
        self.valid_insem_df  = self.create_valid_insem_df()
        self.insem_df        = self.create_insem_df()
        
        self.valid_ultra_df  = self.create_valid_ultra_df()
        self.ultra_df       = self.create_ultra_df()
        
        self.df2             = self.create_df2()
        # self.df3             = self.create_df3()

    def create_bd(self):
        bd = self.bd1[['adj_bdate', 'death_date']]
        bd.rename(columns={'adj_bdate': 'b_date'})
        return bd

    def create_first_calf(self):
        first_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        first_calf1.rename(columns={'calf#': 'first calf#',
                        'b_date': 'first calf bdate'},
                        inplace=True)
        first_calf = self.bd.merge(right=first_calf1, on='WY_id', how='outer', suffixes=('', "_right") )
        first_calf.drop([i for i in first_calf.columns if '_right' in i], axis=1, inplace=True)
        return first_calf
    

    def create_last_calf(self):
        last_calf1 = self.lb.groupby('WY_id').agg({
            'b_date'  : 'max',
            'calf#'   : 'max'
            }).reset_index()
        last_calf1.rename(columns={'calf#': 'last calf#',
            'b_date': 'last calf bdate'}, inplace=True)
        last_calf = self.bd.merge(right=last_calf1, on='WY_id', how='outer', suffixes=('', "_right") )
        last_calf.drop([i for i in last_calf.columns if '_right' in i], axis=1, inplace=True)
        return last_calf
    

    def create_last_stop(self):
        last_stop1 = self.s.groupby('WY_id').agg({
            'lact_num'    : 'max',
            'stop'        : 'max'
        })   
        last_stop1.rename(columns = {'lact_num':'stop calf#',
                'stop':'last stop date'
                },inplace=True)
        last_stop = self.bd.merge(right=last_stop1, on='WY_id', how='outer', suffixes=('', "_right") )
        last_stop.drop([i for i in last_stop.columns if '_right' in i], axis=1, inplace=True)
        last_stop.set_index='WY_id'
        last_stop['last stop date'].fillna(np.nan, inplace=True)
        return last_stop
    
    
    #merge bd with last stop
    def create_df(self):
        df = self.bd.merge(right= self.last_stop, on='WY_id', how='outer', suffixes=('', "_right"))
        df.drop([i for i in df.columns if '_right' in i], axis=1, inplace=True)
        return df



    def create_last_insem(self):
        i1 = self.i.groupby('WY_id',as_index=True).agg({
        'calf_num':'max',
        'insem_date':'max',
        'readex'    : lambda x: x.iloc[0]   
        
        })
        i1.rename(columns = {'calf_num'     :'i_calf#',
                            'insem_date'   :'i_date1',
                            'try_num'      : 'try#'
                            },inplace=True)
        last_insem = self.bd.merge(right=i1, on='WY_id', how='outer', suffixes=('', "_right") )
        last_insem.drop([i for i in last_insem.columns if '_right' in i], axis=1, inplace=True)
        last_insem.set_index='WY_id'
        last_insem['i_date1'].fillna(np.nan, inplace=True)
        return last_insem
    
        
    def create_last_ultra(self):
        u1 = self.u.groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max',
        'readex'  : lambda x: x.iloc[0]   
        })
        u1.rename(columns = {'Calf_num'  :'u_calf#',
                            'ultra_date':'u_date1',
                            'Try_num'   : 'try#'
                            },inplace=True)
        last_ultra = self.bd.merge(right=u1, on='WY_id', how='outer', suffixes=('', "_right") )
        last_ultra.drop([i for i in last_ultra.columns if '_right' in i], axis=1, inplace=True)
        last_ultra.set_index ='WY_id'
        last_ultra['u_date1'].fillna(np.nan, inplace=True)
        return last_ultra
    


    def create_valid_insem_df(self):
        valid_insem_mask = (self.last_insem['i_date1'] > self.last_stop['last stop date'])
        valid_insem_mask =  self.last_insem['i_date1'][valid_insem_mask]  
        valid_insem_df   = self.df.merge(right=valid_insem_mask, on='WY_id', how='outer', suffixes=('', "_right"))
        valid_insem_df.drop([i for i in valid_insem_df.columns if '_right' in i], axis=1, inplace=True)
        return valid_insem_df


    # combines all insem: last, valid, and age 
    def create_insem_df(self):  
        age_insem1 = self.valid_insem_df.loc[(self.valid_insem_df['i_date1'].notnull())].copy()
        age_insem1['age_insem'] =  (self.today - age_insem1['i_date1']).dt.days
        insem_df = self.valid_insem_df.merge(right=age_insem1, on='WY_id', how='outer', suffixes=('', "_right"))
        
        insem_df.drop([i for i in insem_df.columns if '_right' in i], axis=1, inplace=True)
        return insem_df


       
    def create_valid_ultra_df(self):
        valid_ultra_mask = ((self.last_ultra['u_date1'] > self.last_stop['last stop date'])
                            & (self.last_ultra['u_date1'] > self.valid_insem_df['i_date1'])
                            )
        
        valid_ultra_mask = self.last_ultra['u_date1'][valid_ultra_mask]  
        valid_ultra_df   = self.df.merge(right=valid_ultra_mask, on='WY_id', how='outer', suffixes=('', "_right"))
        valid_ultra_df.drop([i for i in valid_ultra_df.columns if '_right' in i], axis=1, inplace=True)
        return valid_ultra_df
    
    
    
    # combines all ultra:  last, valid and age
    def create_ultra_df(self)    :
        age_ultra1 = self.valid_ultra_df.loc[(self.valid_ultra_df['u_date1'].notnull())].copy()
        age_ultra1['age_ultra'] =  (self.today - age_ultra1['u_date1']).dt.days
        ultra_df = self.valid_insem_df.merge(right=age_ultra1, on='WY_id', how='outer', suffixes=('', "_right"))
        
        ultra_df.drop([i for i in ultra_df.columns if '_right' in i], axis=1, inplace=True)
        return ultra_df
    
    
        #merge insem_df with ultra_df
    def create_df2(self):
        insem_cols = list(self.insem_df.columns)
        ultra_cols = list(self.ultra_df.columns)

        df2 =pd.merge( self.insem_df, self.ultra_df, on='WY_id',  suffixes=('', "_right"))
        df2.drop([i for i in df2.columns if '_right' in i], axis=1, inplace=True)
        return df2
    






     

