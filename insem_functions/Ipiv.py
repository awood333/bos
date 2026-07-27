import inspect
from pathlib import Path
import pandas as pd
from container import get_dependency

class Ipiv:
    def __init__(self):
        print(f"Ipiv instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.IUB = None
        self.IUD = None
        self.insem = None
        self.alive_ids = None
        self.ipiv_milking = None
        self.ipiv_milkers = None

    def load(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.IUB = get_dependency('Insem_ultra_basics')
        self.IUD = get_dependency('insem_ultra_data')
        self.process()
        
    def process(self):
        self.insem = self.IUB.data['i']
        alive_ids1 = self.IUB.data['bd'].loc[self.IUB.data['bd']['death_date'].isnull()]
        alive_ids2 = alive_ids1.reset_index()
        self.alive_ids = alive_ids2['wy_id']

        self.ipiv_milking = self.create_ipiv()
        self.ipiv_milkers = self.add_cols_from_allx()

  
    def create_ipiv(self):
        lc1 = self.IUB.last_calf.reset_index()
        lc2 = lc1[['wy_id', 'last_calf_num']].copy()
        # lc2['last_calf_num'] += 1
        lc = lc2.rename(columns={'last_calf_num' : 'lact#'})
         
       
        # Filter with alive_ids
        this_calf = lc[lc['wy_id'].isin(self.alive_ids)].reset_index(drop=True)
        #unnecessary.....
        # this_calf['wy_id'] = pd.to_numeric(this_calf['wy_id'], errors='coerce').dropna().astype(int)
        # this_calf['lact#'] = pd.to_numeric(this_calf['lact#'], errors='coerce').dropna().astype(int)

        insem1 = self.insem.copy()
        insem1['calf_num'] = insem1['calf_num'].fillna('0').astype(int)
        
        #this_calf1 adds the try_nums to the 'last_calf' (now called 'lact#)
        this_calf1 = this_calf.merge(insem1,
                                      left_on=['wy_id', 'lact#'],
                                      right_on=['wy_id', 'calf_num'],
                                      how='left')

        this_calf2 = this_calf1.drop(columns=['calf_num','typex', 'readex'])
        #unnecessary
        # this_calf2['try_num'] = pd.to_numeric(this_calf2['try_num'], errors='coerce').fillna(1).astype(int)
        # this_calf2['insem_date'] = pd.to_datetime(this_calf2['insem_date'], errors='coerce') #
        
        ipiv_milking1 = pd.pivot_table(this_calf2,
            values='insem_date',
            index=['wy_id'],
            columns='try_num',
            aggfunc='first',
            dropna=False 
        )
        
        ipiv_milking1.index = ipiv_milking1.index.astype(str)
        self.ipiv_milking = ipiv_milking1.sort_index()
             
        return self.ipiv_milking
    
    def add_cols_from_allx(self):
        
        xxx = self.IUD.allx[['wy_id', 'u_read', 'days_milking']].set_index('wy_id', drop=True)
        xxx.index = xxx.index.astype(int).astype(str)
        
        ipiv_milkers1 = pd.merge(xxx, self.ipiv_milking, how='right', left_index=True, right_index=True)
        ipiv_milkers1.index = ipiv_milkers1.index.astype(int)

        ipiv_milkers2 = ipiv_milkers1.sort_index()
                
        self.ipiv_milkers = ipiv_milkers2 
        return self.ipiv_milkers 

if __name__ == "__main__":
    obj=Ipiv()
    obj.load()
    