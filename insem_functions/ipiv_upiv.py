import pandas as pd 

from insem_functions.insem_ultra_basics import InsemUltraBasics
from MilkBasics import MilkBasics
class Pivots:
    def __init__ (self):
        
        self.IUB = InsemUltraBasics()
        MB  = MilkBasics()
        
        alive_ids1      = MB.data['bd'][MB.data['bd']['death_date'].isnull()]
        alive_ids2 = alive_ids1.reset_index()
        self.alive_ids = alive_ids2['WY_id']
        self.ipiv = self.create_ipiv()
        
        
    def create_ipiv(self):
        lc = self.IUB.last_calf
        
        # Filter with alive_ids
        last_calf1 = lc[lc['WY_id'].isin(self.alive_ids)]
        last_calf = last_calf1.reset_index(drop=True)

        # insem_data = self.IUB.data['i']
        insem_data_grouped = self.IUB.data['i'].groupby('WY_id').agg({
            'calf_num': 'max',
            'try_num': 'max',
            'insem_date': 'max',
            'typex': 'first',
            'readex': 'first'
        }).reset_index()
        
        merged_data1 = last_calf.merge(insem_data_grouped, 
                                      on='WY_id', 
                                      how='left')
        
        merged_data = merged_data1[['WY_id','last calf#','try_num','insem_date']]
        
        self.ipiv = pd.pivot_table(merged_data,
            values='insem_date',
            index=['WY_id', 'last calf#'],
            columns='try_num',
            aggfunc='first'
        )
                                   
                                   
        self.ipiv.to_csv('F:\\COWS\\data\\insem_data\\ipiv.csv')                
        return self.ipiv
    
    
if __name__ == "__main__":
    piv = Pivots()