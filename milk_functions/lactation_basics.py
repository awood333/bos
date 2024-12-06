'''lactations.py'''

import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import numpy as np
from MilkBasics    import MilkBasics
from milk_functions.WetDry          import WetDry
from utilities.override_print_limit import override_print_limits

override_print_limits()

class Lactation_basics:

    def __init__(self):
        
        self.MB     = MilkBasics()
        self.WD     = WetDry()
        
      
        self.lactations_array, self.headers = self.create_lactation_basics()
        

    def create_lactation_basics(self):

        milkx = self.MB.data['milk']
        ext_range = self.MB.data['ext_rng']
        milk = milkx.reindex(ext_range)
        
        
        lastday = self.MB.data['lastday']
        startx= self.MB.data['start']
        stopx = self.MB.data['stop']
        stop_cols = list(self.MB.data['stop'].columns)  # these will be the headers in stop, i.e., the lactations

        milk_cols1 = milk.columns
        lact = lactations = []
        model = pd.DataFrame(index=range(1,1000))

        for i in startx.index:   #lactations  
            lact = {}
            milk3a = pd.DataFrame(index=range(1,1000)).fillna(0).infer_objects(copy=False)
            
            for j in startx.columns:    #WY_ids
                
                start = startx.loc[i,j]
                stop = stopx.loc[i,j]

                if pd.isna(start) and pd.isna(stop):  # start doesn't exist and stop doesn't exist - continue, in order to hold the place
                    continue

                if pd.isna (stop):
                    stop = lastday
                    
                milk1 = pd.DataFrame(milk.loc[start:stop, str(j)])
                # print('milk1: ',milk1[:5])
                
                
                if not milk1.empty:
                    milk1a = milk1.reset_index(drop=True)
                    milk1a.index += 1
                    # print('milk1a: ',milk1a[:5])
                    milk2 = model.merge(
                        milk1a, left_index=True, right_index=True, 
                        how='left')
                    milk2 = milk2.fillna(0).infer_objects(copy=False)
                    
                elif  milk1.empty:
                    milk2 = model
                    
                milk2.name = j
                
                # print('milk2: ',milk2[:5])
                # milk1.to_csv('F:\\COWS\\data\\milk_data\\lactations\\milk1.csv')
                # milk2.to_csv('F:\\COWS\\data\\milk_data\\lactations\\milk2.csv')
   
                milk3a  = pd.concat([milk3a, milk2], axis=1)
                # print('milk3a: ',milk3a[:10])                
                                
                milk2 = pd.Series()
            
            milk3b1 = milk3a.T.reset_index(drop=False)
            # milk3b1['index'] = milk3b1['index'].astype(int)
            milk3b1.index += 1
            milk3b = milk3b1.T
            # print('milk3b: ',milk3b.iloc[:5,:]) 
            
            

            milk3c = model.merge(
                milk3b, left_index=True, right_index=True, 
                how='left')
            milk3 = milk3c.fillna(0) .infer_objects(copy=False)
            # print('milk3c: ',milk3c.iloc[:5,:]) 
    
            milk3_col_count = milk3.shape[1]
            col_pad            = milk3.shape[1] - milk3_col_count
            
            if col_pad > 0:
                for _ in range(col_pad):
                    milk3[f'pad_{_}'] = 0
            
            if milk3.shape[1] > 0:
                lact[i] =  np.array(milk3.fillna(0).infer_objects(copy=False))
                if lact[i].size == 0:
                    lact[i] = np.zeros((1000, len(startx.shape[1])))
                else:
                    # Pad lact[i] to ensure it has the shape (1000, startx.shape[1])
                    padded_lact = np.zeros((1000, startx.shape[1]))
                    padded_lact[:lact[i].shape[0], :lact[i].shape[1]] = lact[i]
                    lact[i] = padded_lact
                    
                
                print(f'lact {i} shape: {lact[i].shape}')
                lactations.append(lact[i])
                
            milk3.to_csv('F:\\COWS\\data\\milk_data\\lactations\\milk3.csv')            
            milk3 = pd.DataFrame(index=range(1,1000)).fillna(0).infer_objects(copy=False)
        
        self.headers = milk_cols1        
        self.lactations_array = np.array(lactations)

        return self.lactations_array, self.headers

if __name__ == "__main__":
    Lactation_basics()