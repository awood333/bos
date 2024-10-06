import pandas as pd
import numpy as np
from datetime import datetime, date


i2 = pd.read_csv('F:\\COWS\\data\\csv_files\\insem.csv',index_col=None,header=0,parse_dates=['insem_date'], dtype={'readex': 'object'})
u2 = pd.read_csv('F:\\COWS\\data\\csv_files\\ultra.csv',index_col=None,header=0,parse_dates=['ultra_date'])
lb2 = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv',index_col=None,header=0,parse_dates=['b_date'])
# mc2 = pd.read_csv('F:\\COWS\\data\\csv_files\\miscarries.csv',index_col=None,header=0,parse_dates=['miscarry_date'])
sd2 = pd.read_csv('F:\\COWS\\data\\csv_files\\stop_dates.csv',index_col=None,header=0,parse_dates=['stop'])
bd2 = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',index_col=None,header=0,parse_dates=['birth_date', 'death_date', 'adj_bdate'])
#
u2.rename(columns={'ultra_date':'datex','Calf_num':'calf_num','Try_num':'try_num','ultrareading':'readex'},inplace=True)
i2.rename(columns={'insem_date':'datex'},inplace=True)
lb2.rename(columns={'b_date':'datex','calf#':'calf_num'},inplace=True)
# mc2.rename(columns={'miscarry_date':'datex'},inplace=True)
sd2.rename(columns={'stop':'datex','lact_num':'calf_num'},inplace=True)
bd2.rename(columns = {'death_date' : 'datex'}, inplace=True)
#
u =  u2.drop(['try_num', 'memo'] ,axis=1)
i =  i2.drop(['try_num'] ,axis=1)
lb = lb2.drop(['try#']    ,axis=1)
# mc =mc2
sd = sd2
bd = bd2.drop(['birth_date','dam_num','arrived','adj_bdate'], axis=1)
#
i['datex']  = pd.to_datetime(i['datex']  ,format='%m,%d,%Y')
u['datex']  = pd.to_datetime(u['datex']  ,format='%m,%d,%Y')
lb['datex'] = pd.to_datetime(lb['datex'] ,format='%m,%d,%Y')
# mc['datex'] = pd.to_datetime(mc['datex'] ,format='%m,%d,%Y')
sd['datex'] = pd.to_datetime(sd['datex'] ,format='%m,%d,%Y')
bd['datex'] = pd.to_datetime(sd['datex'] ,format='%m,%d,%Y')


class I_U_merge:
    def __init__(self):
        self.iu = self.create_basics()
        self.create_write_to_csv() 

    def create_basics(self):

        iu1 = i.merge(u,how='outer')

        iu = pd.concat([iu1, lb, sd, bd ], axis=0, ignore_index=False).\
            sort_values(['WY_id','calf_num','datex', 'readex']).\
            reset_index(drop=True)
        
        return iu
        
    def create_write_to_csv(self):
        self.iu    .to_csv('F:\\COWS\\data\\insem_data\\IU_merge\\IU_merge.csv')
