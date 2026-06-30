'''LactationBasics.py'''
import inspect
import pandas as pd
import numpy as np
from container import get_dependency

class LactationBasics:

    def __init__(self):
        print(f"LactationBasics instantiated by: {inspect.stack()[1].filename}") 
        self.MB = None
        self.lactations_array = None
        self.headers = None

    def load_and_process(self):
        self.MB  = get_dependency('milk_basics')
        self.MAB = get_dependency('milk_aggregates_basic')  # populates MB.data['milk'] with fresh fullday
        self.lactations_array, self.headers = self.create_lactation_basics()

    def create_lactation_basics(self):
        milkx = self.MAB.fullday
        ext_range = self.MB.data['ext_rng']
        milk = milkx.reindex(ext_range)
        
        lastday = self.MB.data['lastday']
        
        lact_start1 = self.MB.data['lb'].loc[:,['wy_id', 'b_date', 'calf_num']]
        lact_start1['b_date'] = pd.to_datetime(lact_start1['b_date'])
        lact_start2 = lact_start1.pivot_table(
            index   ='wy_id',
            columns ='calf_num',
            values  ='b_date',
            aggfunc ='first'  
        )
        
        lact_stop1 = self.MB.data['stop'].loc[:,['wy_id', 'stop_date', 'lact_num']]
        lact_stop1['stop_date'] = pd.to_datetime(lact_stop1['stop_date'])        
        lact_stop2 = lact_stop1.pivot_table(
            index   ='wy_id',
            columns ='lact_num',
            values  ='stop_date',
            aggfunc ='first'  
        )

        lact,  lactations = [],[]
        model = pd.DataFrame(index=range(0,999))
    
        WY_int = milkx.columns
        # WY_str = [str(i) for i in WY_int]
        lacts_str = [str(col) for col in lact_start2.columns]
        lacts_int = [1,2,3,4,5,6]

        milk3 = model

        for i in WY_int:   #iterates down the wy's in the pivot table
            lact = {}
            # milk3 = model
            
            for j in lacts_int:
                start = np.nan
                stop  = np.nan
                missing = False
                
                #if wy_id is missing from livebirths
                if i not in lact_start2.index or j not in lact_start2.columns:
                    missing = True
                #if wy_id is missing from stop_dates                    
                if i not in lact_stop2.index or j not in lact_stop2.columns:
                    missing = True
                if not missing:
                    start = lact_start2.loc[i, j]
                    stop  = lact_stop2.loc[i, j]

                # If missing from one or both start/stop are NaN -> create blank (zero) column
                if missing or (pd.isna(start) and pd.isna(stop)):
                    milk2 = model.copy()  # model is (999,1) index 1..999? Actually it's 1..999 (999 rows) but we need 1000? Check: model = pd.DataFrame(index=range(1,1000)) -> 999 indices. That's a bug, but we'll preserve existing logic.
                    milk2[''] = 0.0
                    milk2.name = j
                    milk3 = pd.concat([milk3, milk2], axis=1)
                    continue

                if not pd.isna(start) and pd.isna (stop):
                    stop = lastday
                    
                milk1 = pd.DataFrame(milk.loc[start:stop, j])
                
                if not milk1.empty:
                    milk1 = milk1.reset_index(drop=True)

                    milk2 = model.merge(
                        milk1, left_index=True, right_index=True, 
                        how='left')
                    milk2 = milk2.fillna(0).infer_objects() #temp var so no copy is more efficient
                    
                elif  milk1.empty:
                    milk2 = model
                    
                milk2.name = j

                milk3  = pd.concat([milk3, milk2], axis=1)
                milk2 = pd.Series()

            if milk3.shape[1] > 0:
                arr = np.array(milk3.fillna(0).infer_objects())
                if arr.ndim == 1:
                    arr = arr.reshape(-1, 1)
                # Target shape (1000, 6)
                arr_target = np.zeros((1000, 6))
                rows = min(arr.shape[0], 1000)
                cols = min(arr.shape[1], 6)
                arr_target[:rows, :cols] = arr[:rows, :cols]
                arr = arr_target
                lact[i] = arr
                lactations.append(lact[i])

            #    reinitialize milk3
            milk3 = pd.DataFrame(index=range(1,1000)).fillna(0).infer_objects()
        
        self.headers = lacts_str        
        self.lactations_array = np.array(lactations).transpose(1,0,2) #the params are the axes
        lactations_array2 = np.array(lactations).transpose(1,0,2)
        
        print('lactations array',self.lactations_array.shape)
        return self.lactations_array, self.headers

if __name__ == "__main__":
    obj = LactationBasics()
    obj.load_and_process()    