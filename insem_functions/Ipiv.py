import pandas as pd 

from insem_functions.insem_ultra_basics import InsemUltraBasics
from insem_functions.Insem_ultra_data   import InsemUltraData
from MilkBasics import MilkBasics
from insem_functions.statusData2 import StatusData2

class Ipiv:
    def __init__ (self):
        
        self.IUB = InsemUltraBasics()
        self.IUD = InsemUltraData()
        
        self.MB  = MilkBasics()
        SD  = StatusData2()
        
        
        self.status = SD.status_col
        self.insem = self.MB.data['i']
        alive_ids1      = self.MB.data['bd'][self.MB.data['bd']['death_date'].isnull()]
        alive_ids2 = alive_ids1.reset_index()
        self.alive_ids = alive_ids2['WY_id']
        
        self.ipiv = self.create_ipiv()        

  
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
        
        self.ipiv = pd.pivot_table(this_calf2,
            values='insem_date',
            index=['WY_id', 'lact#'],
            columns='try_num'
        )
                                   
        self.ipiv.to_csv('F:\\COWS\\data\\insem_data\\ipiv.csv')                
        return self.ipiv
    
    
if __name__ == "__main__":
    Ipiv()