import inspect
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

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.IUB = get_dependency('insem_ultra_basics')
        self.IUD = get_dependency('insem_ultra_data')

        self.insem = self.IUB.data['i']
        alive_ids1 = self.IUB.data['bd'].loc[self.IUB.data['bd']['death_date'].isnull()]
        alive_ids2 = alive_ids1.reset_index()
        self.alive_ids = alive_ids2['WY_id']

        self.ipiv_milking = self.create_ipiv()
        self.ipiv_milkers = self.add_cols_from_allx()
        self.write_to_csv()

  
    def create_ipiv(self):
        lc = self.IUB.last_calf[['WY_id', 'last calf#']].copy()
        lc['last calf#'] += 1
        lc = lc.rename(columns={'last calf#' : 'lact#'})
         
       
        # Filter with alive_ids
        this_calf = lc[lc['WY_id'].isin(self.alive_ids)].reset_index(drop=True)
        this_calf['WY_id'] = pd.to_numeric(this_calf['WY_id'], errors='coerce').dropna().astype(int)
        this_calf['lact#'] = pd.to_numeric(this_calf['lact#'], errors='coerce').dropna().astype(int)

        insem1 = self.insem.copy()
        insem1['calf_num'] = insem1['calf_num'].fillna('0').astype(int)
        
        #this_calf1 adds the try_nums to the 'last_calf' (now called 'lact#)
        this_calf1 = this_calf.merge(insem1,
                                      left_on=['WY_id', 'lact#'],
                                      right_on=['WY_id', 'calf_num'],
                                      how='left')

        this_calf2 = this_calf1.drop(columns=['calf_num','typex', 'readex'])
        
        this_calf2['try_num'] = this_calf2['try_num'].fillna(1).astype(int)
        this_calf2['insem_date'] = pd.to_datetime(this_calf2['insem_date'], errors='coerce') #
        
        ipiv_milking1 = pd.pivot_table(this_calf2,
            values='insem_date',
            index=['WY_id'],
            columns='try_num',
            aggfunc='first',
            dropna=False 
        )
        
        ipiv_milking1.index = ipiv_milking1.index.astype(str)
        self.ipiv_milking = ipiv_milking1.sort_index()
             
        return self.ipiv_milking
    
    def add_cols_from_allx(self):
        
        xxx = self.IUD.allx[['WY_id', 'u_read', 'days milking']].set_index('WY_id', drop=True)
        xxx.index = xxx.index.astype(int).astype(str)
   

        
        ipiv_milkers1 = pd.merge(xxx, self.ipiv_milking, how='right', left_index=True, right_index=True)
        ipiv_milkers1.index = ipiv_milkers1.index.astype(int)

        ipiv_milkers2 = ipiv_milkers1.sort_index()

        # Move 'u_read' and 'days milking' after 'WY_id'
        # cols = ipiv_milkers2.columns.tolist()
        # for col in ['u_read', 'days milking']:
        #     cols.remove(col)
        # cols = cols[:1] + ['u_read', 'days milking'] + cols[1:]
        # ipiv_milkers1 = ipiv_milkers2[cols]
                
        self.ipiv_milkers = ipiv_milkers2 
        return self.ipiv_milkers
    
    def write_to_csv(self):
        self.ipiv_milkers.to_csv('F:\\COWS\\data\\insem_data\\ipiv_milkers.csv')   
    
    
if __name__ == "__main__":
    obj=Ipiv()
    obj.load_and_process()     