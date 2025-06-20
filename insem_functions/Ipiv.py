import pandas as pd 

from insem_functions.insem_ultra_basics import InsemUltraBasics
from insem_functions.Insem_ultra_data   import InsemUltraData
from MilkBasics import MilkBasics
from status_functions.statusData2 import StatusData2

class Ipiv:
    def __init__ (self):
        
        self.IUB = InsemUltraBasics()
        self.IUD = InsemUltraData()
        
        self.MB  = MilkBasics()
        self.SD2  = StatusData2()
        
        self.insem      = self.MB.data['i']
        alive_ids1      = self.MB.data['bd'][self.MB.data['bd']['death_date'].isnull()]
        alive_ids2      = alive_ids1.reset_index()
        self.alive_ids  = alive_ids2['WY_id']
        
        self.ipiv_all_basic   = self.create_ipiv()  
        self.ipiv_milkers       = self.limit_ipiv_to_milkers()  
        self.write_to_csv()

  
    def create_ipiv(self):
        lc = self.IUB.last_calf[['WY_id', 'last calf#']].copy()
        lc['last calf#'] += 1
        lc = lc.rename(columns={'last calf#' : 'lact#'})
        
        # Filter with alive_ids
        this_calf = lc[lc['WY_id'].isin(self.alive_ids)]
        
        insem1 = self.insem.copy()
        insem1['calf_num'] = insem1['calf_num'].fillna('0').astype(float)
        
        this_calf1 = this_calf.merge(insem1,
                                      left_on=['WY_id', 'lact#'],
                                      right_on=['WY_id', 'calf_num'],
                                      how='left')

        this_calf2 = this_calf1.drop(columns=['calf_num','typex', 'readex'])
        
        self.ipiv_all_basic = pd.pivot_table(this_calf2,
            values='insem_date',
            index=['WY_id', 'lact#'],
            columns='try_num'
        )
                                   
             
        return self.ipiv_all_basic
    
    def limit_ipiv_to_milkers(self):
        
        xxx = self.IUD.allx[['WY_id', 'u_read', 'days milking']]
        
        self.ipiv_milkers = xxx.merge(self.ipiv_all_basic, how='left', right_on='WY_id', left_on='WY_id')
        return self.ipiv_milkers
    
    def write_to_csv(self):
        self.ipiv_all_basic.to_csv('F:\\COWS\\data\\insem_data\\ipiv_all_basic.csv')
        self.ipiv_milkers.to_csv('F:\\COWS\\data\\insem_data\\ipiv_milkers.csv')   
    
    
if __name__ == "__main__":
    Ipiv()