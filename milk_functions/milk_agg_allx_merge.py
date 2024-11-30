'''milk_functions//milk_agg_allx_merge.py'''
import pandas as pd 

from milk_functions.milkaggregates import MilkAggregates
from insem_functions.Insem_ultra_data import  InsemUltraData

def MilkAggAllxMerge():
        
    MA = MilkAggregates()
    IUD = InsemUltraData()
    
    tenday = MA.tenday
    allxx = IUD.allx.loc[:,['WY_id', 'days milking']]
    
    x = tenday.merge(allxx,
                     how='left',
                     on ='WY_id',
                     suffixes=('', '_right') 
                     )
    x2 = pd.DataFrame(x)
    return x2
    
    
if __name__ == "__main__":
    MilkAggAllxMerge()