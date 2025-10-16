
import inspect
import pandas as pd
from milk_basics import MilkBasics
from container import get_dependency

class I_U_merge:
    def __init__(self):
        
        print(f"I_U_merge instantiated by: {inspect.stack()[1].filename}")
        
        self.MB         = MilkBasics()
        self.data = self.MB.data
        self.iu = self.create_basics()
        self.create_write_to_csv() 

    def create_basics(self):
        
        b1 = self.data['bd'].copy()
        b2 = b1.drop(columns=(['birth_date','death_date', 'dam_num', 'arrived']))
        b2= b2.rename(columns={'adj_bdate' : 'datex'})
        b2['typex']= 'cow_birth'
        
        d1 = self.data['bd'].copy()
        d2 = d1.drop(columns=(['birth_date','dam_num', 'arrived', 'adj_bdate']))
        d2= d2.rename(columns={'death_date' : 'datex'})        
        d2['typex']= 'cow_death'
        
        lb1 = self.data['lb'].copy()
        lb1 = lb1.drop(columns=(['try#']))        
        lb2 = lb1.rename(columns={'b_date' : 'datex'})  
        lb2['typex'] = 'calf_birth'  
        
        u1 = self.data['u'].copy()
        u2 = u1.rename(columns={'ultra_date' : 'datex', 'calf_num' : 'u-calf#'}) 
        u2['typex'] = 'ultra' 
        
        i1 = self.data['i'].copy()
        i2 = i1.rename(columns={'insem_date' : 'datex', 'calf_num' : 'i_calf#'})
        i2['typex'] = 'insem'
        
        s1 = self.data['stopx'].copy()
        s2 = s1.rename(columns={ 'lact_num' : 'stop#', 'stop' : 'datex'})  


        self.iu = pd.concat([b2, i2, u2, lb2, s2, d2 ], axis=0, ignore_index=False).\
            sort_values(['WY_id','datex']).reset_index(drop=True)

        
        return self.iu
        
    def create_write_to_csv(self):
        self.iu    .to_csv('F:\\COWS\\data\\insem_data\\IU_merge\\IU_merge.csv')
        
if __name__ == '__main__':
    IUM = I_U_merge()
