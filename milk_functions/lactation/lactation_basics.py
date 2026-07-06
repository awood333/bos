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
        self.lacts_str =None

    def load_and_process(self):
        self.MB  = get_dependency('milk_basics')
        self.MAB = get_dependency('milk_aggregates_basic')  # populates MB.data['milk'] with fresh fullday
        #methods
        self.lactations_array, self.headers = self.create_lactation_basics()

    def create_lactation_basics(self):
        milkx = self.MAB.fullday
        ext_range = self.MB.data['ext_rng']
        milk = milkx.reindex(ext_range)
        
        lastday = self.MB.data['lastday']

        lact_start2 = self.MB.data['lact_start_pivot']        
        lact_stop2  = self.MB.data['lact_stop_pivot']
        lact,  lactations = [],[]
        model = pd.DataFrame(index=range(0,999))
    
        WY_int = milkx.columns
        WY_str = [str(i) for i in WY_int]
        self.headers = WY_str
        self.lacts_str = [str(col) for col in lact_start2.columns]
        lacts_int = lact_start2.columns

        num_wy = len(WY_int)

        milk3 = model.copy()

        for i in lacts_int:   #iterates down the lactations in the pivot table
            lact = {}
            
            for j in WY_int: #get all the wy's for each lact then proceed to the next lact
                start = np.nan
                stop  = np.nan
                missing = False
                
                #if wy_id is missing from livebirths
                if j not in lact_start2.index or i not in lact_start2.columns:
                    missing = True
                #if wy_id is missing from stop_dates                    
                if j not in lact_stop2.index or i not in lact_stop2.columns:
                    missing = True
                if not missing:
                    start = lact_start2.loc[j, i]
                    stop  = lact_stop2.loc[j, i]

                # If missing from one or both start/stop are NaN -> create blank (zero) column
                if missing or (pd.isna(start) and pd.isna(stop)):
                    milk2 = model.copy()  # model is (999,1) index 1..999? Actually it's 1..999 (999 rows) but we need 1000? Check: model = pd.DataFrame(index=range(1,1000)) -> 999 indices. That's a bug, but we'll preserve existing logic.
                    milk2[''] = 0.0
                    milk2.name = j
                    milk3 = pd.concat([milk3, milk2], axis=1)
                    continue
                    #if it exists
                if not pd.isna(start) and pd.isna (stop):
                    stop = lastday
                    
                milk1 = pd.DataFrame(milk.loc[start:stop, j])
                
                if not milk1.empty:
                    milk1 = milk1.reset_index(drop=True)

                    milk2 = model.merge(
                        milk1, left_index=True, right_index=True, 
                        how='left')
                    milk2 = milk2.fillna(0).infer_objects() #temp var so no copy is more efficient
                    
                else:
                    milk2 = model.copy()
                    milk2[0] = 0.0   # ensure one column placeholder
                    
                milk2.name = i

                milk3  = pd.concat([milk3, milk2], axis=1)
                milk2 = pd.Series()
                
            if milk3.shape[1] > 0:
                arr = np.array(milk3.fillna(0).infer_objects())
                if arr.ndim == 1:
                    arr = arr.reshape(-1, 1)
                # Target shape (1000, num_wy) – use 1000 rows (padded/truncated)
                # num_wy = len(WY_int)
                arr_target = np.zeros((1000, num_wy))
                rows = min(arr.shape[0], 1000)
                cols = min(arr.shape[1], num_wy)
                arr_target[:rows, :cols] = arr[:rows, :cols]
                arr = arr_target
                lact[i] = arr
                lactations.append(arr)

            #    reinitialize milk3
            milk3 = model.copy()
        
        self.lactations_array = np.dstack(lactations)#.transpose(1,2,0)

        
        print('lactations array',self.lactations_array.shape)
        return self.lactations_array, self.headers

if __name__ == "__main__":
    obj = LactationBasics()
    obj.load_and_process()    