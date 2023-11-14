import pandas as pd
import numpy as np
import datetime


i2 = pd.read_csv('F:\\COWS\\data\\csv_files\\insem.csv',index_col=None,header=0,parse_dates=['insem_date'])
u2 = pd.read_csv('F:\\COWS\\data\\csv_files\\ultra.csv',index_col=None,header=0,parse_dates=['ultra_date'])
lb2 = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv',index_col=None,header=0,parse_dates=['b_date'])
mc2 = pd.read_csv('F:\\COWS\\data\\csv_files\\miscarries.csv',index_col=None,header=0,parse_dates=['miscarry_date'])
sd2 = pd.read_csv('F:\\COWS\\data\\csv_files\\stop_dates.csv',index_col=None,header=0,parse_dates=['stop'])
#
u2.rename(columns={'ultra_date':'datex','Calf_num':'calf_num','Try_num':'try_num','ultrareading':'readex'},inplace=True)
i2.rename(columns={'insem_date':'datex'},inplace=True)
lb2.rename(columns={'b_date':'datex','calf#':'calf_num'},inplace=True)
mc2.rename(columns={'miscarry_date':'datex'},inplace=True)
sd2.rename(columns={'stop':'datex','lact_num':'calf_num'},inplace=True)
#
u=  u2.drop(['try_num', 'memo'] ,axis=1)
i=  i2.drop(['try_num'] ,axis=1)
lb= lb2.drop(['try#']    ,axis=1)
mc=mc2
sd= sd2
#
i['datex']  =pd.to_datetime(i['datex']  ,format='%m,%d,%Y')
u['datex']  =pd.to_datetime(u['datex']  ,format='%m,%d,%Y')
lb['datex'] =pd.to_datetime(lb['datex'] ,format='%m,%d,%Y')
mc['datex'] =pd.to_datetime(mc['datex'] ,format='%m,%d,%Y')
sd['datex'] =pd.to_datetime(sd['datex'] ,format='%m,%d,%Y')


class I_U_merge:
    def __init__(self):
        self.iu4 = self.create_basics()
    
        
    def create_basics(self):

        iu1 = i.merge(u,how='left',on=["WY_id",'calf_num'],suffixes=('_I','_U'))
        iu2 = iu1.groupby(['WY_id','calf_num']).agg('first')
        iu3 = pd.concat([i,u,lb,mc,sd],axis=0)
        
        iu4 = iu3.groupby(['WY_id','calf_num','datex']).agg('first')
        return iu4
        
        
        
    def create_write_to_csv(self):
        self.iu4.to_csv('F:\\COWS\\data\\insem_data\\i_u_merge.csv')