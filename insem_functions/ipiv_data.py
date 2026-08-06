import inspect
from pathlib import Path
import pandas as pd
from container import get_dependency

class IpivData:
    def __init__(self):
        print(f"IpivData instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.IUB = None
        self.IUD = None
        self.insem = None
        self.alive_ids = None
        self.ipiv_data = None
        self.ipiv_milkers = None

    def load(self):
        self.MB  = get_dependency('milk_basics')
        self.DR  = get_dependency('date_range')
        self.IUB = get_dependency('insem_ultra_basics')
        self.IUD = get_dependency('insem_ultra_data')
        self.process()
        
    def process(self):
        self.insem      = self.IUB.data['i']
        alive_ids1      = self.IUB.data['bd'].loc[self.IUB.data['bd']['death_date'].isnull()]
        alive_ids2      = alive_ids1.reset_index()
        self.alive_ids  = alive_ids2['wy_id']
        
        #methods
        self.ipiv_data  = self.create_this_calf()

  
    def create_this_calf(self):
        lc1 = self.IUB.last_calf.reset_index()
        lc2 = lc1[['wy_id', 'last_calf_num']].copy()
        # lc2['last_calf_num'] += 1
        lc = lc2.rename(columns={'last_calf_num' : 'lact_num'})
         
        # Filter with alive_ids
        this_calf = lc[lc['wy_id'].isin(self.alive_ids)].reset_index(drop=True)
        insem1 = self.insem.copy()
        insem1['calf_num'] = insem1['calf_num'].fillna('0').astype(int)
        
        # this_calf1 adds the try_nums to the 'last_calf' (now called 'lact_num)
        # and it gives us the date of the last insem...........
        this_calf1 = this_calf.merge(insem1,
                                      left_on=['wy_id', 'lact_num'],
                                      right_on=['wy_id', 'calf_num'],
                                      how='left')

        this_calf2 = this_calf1.drop(columns=['calf_num','typex', 'readex'])

        this_calf2['try_num'] = this_calf2['try_num'].fillna(0).astype(int)  # in case a NaN try_num sneaks through
        this_calf2['insem_date'] = pd.to_datetime(this_calf2['insem_date'], errors='coerce').dt.date
        self.ipiv_data = this_calf2
        return self.ipiv_data
    


if __name__ == "__main__":
    obj=IpivData()
    obj.load()
    