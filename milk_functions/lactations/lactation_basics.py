'''lactations.py'''

import pandas as pd
import numpy as np
from MilkBasics    import MilkBasics
from status_functions.WetDry          import WetDry
from utilities.override_print_limit import override_print_limits

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
        lact,  lactations = [],[]
        model = pd.DataFrame(index=range(1,1000))
        WY_int = range(0,len(self.MB.data['bd'].index))
        # WY_str  = WY_int.astype(str)
        


        for i in startx.index:   #lactations  
            lact = {}
            milk3a = model
            
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

                milk3a  = pd.concat([milk3a, milk2], axis=1)
                milk2 = pd.Series()
            
            
            milk3b = milk3a.T
            milk3b.index = milk3b.index.astype(int)
           
            milk3b2 = milk3b.reindex(index=WY_int, fill_value=0)
 
            milk3 = milk3b2.T
      
            col_pad = len(milk.columns) - milk3.shape[1]  

            if col_pad > 0:
                for _ in range(col_pad):
                    milk3[f'pad_{_}'] = 0   #The underscore _ is used to indicate that the loop variable is not used elsewhere in the loop.
                                            # adds col_pad number of cols to the milk3 array
            
            
            if milk3.shape[1] > 0:  #array is not empty
                lact[i] =  np.array(milk3.fillna(0).infer_objects(copy=False))
                
                if lact[i].size == 0:   # for an empty array
                    lact[i] = np.zeros((1000, len(startx.shape[1])))
                else:
                    #defines an to ensure it has the shape (1000, startx.shape[1])
                    padded_lact = np.zeros((1000, startx.shape[1])) #creates a zeros array
                    # takes the array in lact[i] and places it in the shape[0] ,shape[1] quadrent 
                    padded_lact[:lact[i].shape[0], :lact[i].shape[1]] = lact[i]
                    # and then redefines lact[i] as the reshaped array
                    lact[i] = padded_lact
                    
                
                # print(f'lact {i} shape: {lact[i].shape}')
                lactations.append(lact[i])
                
            # milk3.to_csv('F:\\COWS\\data\\milk_data\\lactations\\milk3.csv') 
            
            #    reinitialize milk3
            milk3 = pd.DataFrame(index=range(1,1000)).fillna(0).infer_objects(copy=False)
        
        self.headers = milk_cols1        
        self.lactations_array = np.array(lactations)

        return self.lactations_array, self.headers

if __name__ == "__main__":
    Lactation_basics()