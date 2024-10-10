'''wet_dry.py'''

import pandas as pd
import numpy as np

from MilkBasics import MilkBasics

today =  pd.Timestamp.today()

class WetDry:
    def __init__(self):
        
        self.data = MilkBasics().data     

        self.ext_rng = self.data['ext_rng']
        self.milk1 = self.data['milk'].reindex(self.data['ext_rng'])
  
        
                
        self.create_wet_milking()


    def create_wet_milking(self):         

        
        wet1c   = pd.DataFrame()
        wet1d   = pd.DataFrame()
        
        wet2a = pd.DataFrame()
        wet2a.index = self.ext_rng
        wet2_days = pd.DataFrame()
        wet2_days.index = self.ext_rng
        
       
        
                                    
        rows = self.data['stop'].index     #integers
        cols = self.data['stop'].columns  #integers

        for i in rows:  # lact_nums
            for j in cols:         #WY nums
                
                lastday = self.data['lastday']                #last day of the milk df datex
                k       = str(i)
                
                start   = self.data['start'].loc[i,j]
                stop    = self.data['stop'].loc[i,j]


                a =  pd.isna(start) is False        # start value exists
                b =  pd.isna(stop)  is False        # stop value exists
                e =  pd.isna(start) is True        # start value missing
                f =  pd.isna(stop)  is True        # stop value missing

             
                # completed lactation: 
                if a and b:

                    wet1a   = self.milk1.loc[start:stop, str(i)]
                    wet1a.name=None
                    wet_numbering = pd.DataFrame({'num': range(1, len(wet1a.index)+1)}, index=wet1a.index)
                    wet1b = pd.concat([wet1a, wet_numbering], axis=1)               
                    
                    # get sum of series
                    wetsum1 = wet1a.sum()
                    
                    # stack the series vertically
                    wet1c = pd.concat([wet1b, wet1c], axis=0)
                    
                if a and f:
                    
                    stop = lastday

                    wet1a   = self.milk1.loc[start:stop, str(i)]
                    wet1a.name=None
                    wet_numbering = pd.DataFrame({'num': range(1, len(wet1a.index)+1)}, index=wet1a.index)
                    wet1b = pd.concat([wet1a, wet_numbering], axis=1)               
                    
                    # get sum of series
                    wetsum1 = wet1a.sum()
                    
                    # stack the series vertically
                    wet1c = pd.concat([wet1b, wet1c], axis=0)
                    
                   
                   
               
            # reindex
            wet1d = wet1c.reindex(self.ext_rng)


            wet1_days    = wet1d.iloc[:,1:]
            wet1_days = wet1_days .rename (columns={'num': k})


            wet2_days = pd.concat([wet2_days, wet1_days], axis=1)            
            wet1c = pd.DataFrame()
            wet1d = pd.DataFrame()
        
            
        wet2_days.iloc[709:,:].to_csv('F:\\COWS\\data\\status\\wet2_days.csv')
                   
        return

            #

if __name__ == '__main__':
    WetDry()