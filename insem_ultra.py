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
        
        self.df['WY_id'] = self.rng
        self.df.set_index('WY_id',drop=True,inplace=True)
        self.date_format = '%m/%d/%Y'
        
        self.first_calf = self.create_first_calf()
        self.last_calf  = self.create_last_calf()
        self.last_stop  = self.create_last_stop()
        

        self.last_insem = self.create_last_insem()
        self.last_ultra = self.create_last_ultra()
        
        self.last_ultra_insem = self.create_merge_last_ultra_insem()
  
        
        self.valid_insem_df  = self.create_valid_insem_df()
        self.insem_df        = self.create_insem_df()
        
        self.valid_ultra_df  = self.create_valid_ultra_df()
        self.ultra_df        = self.create_ultra_df()
        
        self.df2            = self.merge_insem_ultra()
        self.df3, self.date_cols = self.create_expected_bdate()
        
        self.allx           = self.create_allx()
     
        self.ipiv           = self.create_last_insem_pivot()
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
        
        self.last_insem = self.bd.merge(right=i1,  
                how         ='left', 
                left_index  =True, 
                right_on    ='WY_id', 
                suffixes    =('', "_right") )
        
        self.last_insem.drop([i for i in self.last_insem.columns if '_right' in i], axis=1, inplace=True)
        
        self.last_insem.set_index('WY_id', inplace=True)
        self.last_insem['i_date'].fillna(np.nan, inplace=True)
        return self.last_insem
    
        
    def create_last_ultra(self):
        
        u1 = self.u.groupby('WY_id',as_index=True).agg({
        'Calf_num'      : 'max',
        'ultra_date'    : 'max'
        # 'readex'        : 'first'  
        })
        
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
        
        self.last_ultra = self.bd.merge(
            right       =u1a, 
            how         ='left', 
            left_index  =True, 
            right_on    ='WY_id', 
            suffixes    =('', "_right") )
        
        self.last_ultra.drop([i for i in self.last_ultra.columns if '_right' in i], axis=1, inplace=True)
        
        self.last_ultra.set_index('WY_id', inplace=True)
        self.last_ultra['u_date'].fillna(np.nan, inplace=True)
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
        
        valid_insem_mask1   =  (self.last_insem['i_calf#'] > self.df['last calf#']) 
        valid_insem_mask    =  self.last_insem['i_date'][valid_insem_mask1].to_frame()
        
        valid_insem_mask.reset_index(drop=True, inplace=True)

        valid_insem_df1 = self.last_insem[valid_insem_mask1].copy()
        self.valid_insem_df = valid_insem_df1.reindex(self.rng)
        
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
        # insem_df.set_index('WY_id', inplace=True)  WY_id is already the index
        return self.insem_df


       
    def create_valid_ultra_df(self):
        valid_ultra_mask = ((self.last_ultra['u_date'] > self.valid_insem_df['i_date']) &
                            (
                            (self.valid_insem_df['i_date'].notnull()) | 
                            (self.valid_insem_df['i_date'].notna()) 
                            ) 
          )  # for all cows incl dead
        
        valid_ultra_mask_df1 = self.last_ultra[['u_date', 'u_read']][valid_ultra_mask]                  # filters and attaches date
        valid_ultra_mask_df1.index +=1 
        
        valid_ultra_mask_df = self.bd.merge(valid_ultra_mask_df1, 
                    left_index=True, 
                    right_index=True, 
                    how='left', 
                    suffixes=('_left', "__right") )
        
        valid_ultra_df1 = self.last_ultra[valid_ultra_mask]
        self.valid_ultra_df = valid_ultra_df1.reindex(self.rng)
        
        return self.valid_ultra_df
    
    
    
                                        # combines all ultra:  last, valid and age
    def create_ultra_df(self)    :
        age_ultra1 = self.valid_ultra_df.loc[(self.valid_ultra_df['u_date'].notnull())].copy()
        age_ultra1['age ultra'] =  (self.today - age_ultra1['u_date']).dt.days
        
        self.ultra_df = self.valid_ultra_df.merge(
            right=age_ultra1, 
            on='WY_id', 
            how='left', 
            suffixes=('', "_right"))
        
        self.ultra_df.drop([i for i in self.ultra_df.columns if '_right' in i], axis=1, inplace=True)
        # ultra_df.set_index('WY_id', inplace=True)
        
        return self.ultra_df
    
    
                                        #merge insem_df with ultra_df and with df
    def merge_insem_ultra(self):
        # insem_cols = list(self.insem_df.columns)
        # ultra_cols = list(self.ultra_df.columns)

        df2a =pd.merge( self.insem_df, self.ultra_df, 
                       on       ='WY_id', 
                       how      ='outer', 
                       suffixes =('', "_right"))
        
        df2a.drop([i for i in df2a.columns if '_right' in i], axis=1, inplace=True)
        
        self.df2 = pd.merge(self.df, df2a, on='WY_id',  suffixes=('', "_right") )
        self.df2.drop([i for i in self.df2.columns if '_right' in i], axis=1, inplace=True)
        # df2.set_index('WY_id', inplace=True)
        
        return self.df2
    
    
# expected bdate 
    def create_expected_bdate(self):
        
        bdemask =  (
            (self.df2['u_date'].notnull())  & 
            (self.df2['u_read'] == 'ok')    &
            (self.df2['death_date'].isnull())
            )
                            
        
        bde1 = self.df2[bdemask].copy()
        
        bde1['expected bdate'] = bde1['i_date'] + pd.to_timedelta(282, unit='D')
        bde = bde1['expected bdate']
    
        self.df3  = self.df2.merge(right=bde, on='WY_id', how='left' )
        self.df3.fillna(np.nan, inplace=True)
        
        self.df3.drop(columns= 'adj_bdate', inplace=True)

        self.date_cols = ['death_date','last stop date','last calf bdate','i_date','u_date','expected bdate']
        
       
        return self.df3, self.date_cols
    
    
    def create_allx(self):
        lc = self.last_calf
        tdy = pd.Timestamp.today()
        lastcalf_age = [(tdy - date).days for date in lc['last calf bdate']]
        
        self.df3['last calf age'] = lastcalf_age
        
        self.df3_reset = self.df3.reset_index()
        df4 = self.df3_reset.merge(self.status_col[['ids','status']], how='left', left_on='WY_id', right_on='ids', suffixes=('', '_right'))
        df4.drop(columns=['ids'], inplace=True)
        df4.set_index('WY_id', inplace=True)
        
        df5 = df4.loc[df4['status'] != 'G']
        
        self.allx = df5
        
        return self.allx
    

    
    def create_last_insem_pivot(self):
        i2 = self.i.drop(columns=['typex', 'readex'])
    
        maxcalf1 = self.last_insem[['i_calf#', 'death_date']].copy()
        maxcalf1.rename(columns={'i_calf#': 'maxcalf#'}, inplace=True)
        
        livemask = maxcalf1['death_date'].isnull()

        maxcalf3 = maxcalf1[livemask].fillna(0)
           
        pivdata1 = i2.merge(maxcalf3, 
                            how='right', 
                            left_on=['WY_id', 'calf_num'], 
                            right_on=['WY_id', 'maxcalf#'])
        
        pivdata2 = pivdata1.dropna(subset=['calf_num'])
        
        self.ipiv = pd.pivot_table(pivdata2, 
                    index   = ['WY_id', 'calf_num' ], 
                    columns = ['try_num' ],
                    values  = 'insem_date'  ,
                    aggfunc = 'max'
                    )
        return self.ipiv

    
    def create_write_to_csv(self):
        
        self.last_calf      .to_csv('F:\\COWS\\data\\insem_data\\lb_last.csv')
        self.bd             .to_csv('F:\\COWS\\data\\insem_data\\bd.csv')
  
        self.valid_ultra_df .to_csv('F:\\COWS\\data\\insem_data\\valid_ultra_df.csv')
        self.last_ultra     .to_csv('F:\\COWS\\data\\insem_data\\last_ultra.csv')
        self.ultra_df       .to_csv('F:\\COWS\\data\\insem_data\\ultra_df.csv')
        
        self.last_insem     .to_csv('F:\\COWS\\data\\insem_data\\last_insem.csv')       
        self.valid_insem_df .to_csv('F:\\COWS\\data\\insem_data\\valid_insem.csv')
        
        self.df2            .to_csv('F:\\COWS\\data\\insem_data\\df2.csv')
        
        
        self.last_ultra_insem   .to_csv('F:\\COWS\\data\\insem_data\\last_ultra_insem.csv')
        self.allx               .to_csv('F:\\COWS\\data\\insem_data\\allx.csv')
        self.ipiv               .to_csv ('F:\\COWS\\data\\insem_data\\ipiv.csv')
