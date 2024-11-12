'''lactations.py'''

import pandas as pd    #123   456
import numpy as np
from MilkBasics    import MilkBasics
from milk_functions.WetDry          import WetDry


class Lactation_basics:

    def __init__(self):
        
        self.MB     = MilkBasics()
        self.WD     = WetDry()
      
        self.lact4x, self.day_of_milking4 = self.create_lactation_basics()
        

    def create_lactation_basics(self):

        milk = self.MB.data['milk']
        start= self.MB.data['start']
        stop = self.MB.data['stop']
        lacts1 = list(self.MB.data['stop'].columns)  # these will be the headers in stop, i.e., the lactations

        milk_cols1 = milk.columns
        cols = [int(x) for x in milk_cols1]
        lacts = [int(x) for x in lacts1]
        

        # Initialize arrays to store the data
        max_rows = len(lacts)
        max_cols = len(cols)
        

        

        for j in start.columns:
            for i in lacts:
                start = start.loc[j,i]
                stop = stop.loc[j,i]

                if pd.isna(start) and pd.isna(stop):  # start doesn't exist and stop doesn't exist - skip
                    continue

                date_range = pd.date_range(start=start, end=stop)
                md1 = pd.Series(range(1, len(date_range) + 1), index=date_range)

                # lact1 = milk.loc[start:stop, str(int(j))]
                
                
                md_np1 = np.array(md1)
            md_np2 = np.hstack((md_np2, md_np1))
            

        return self.lact4x, self.day_of_milking4

if __name__ == "__main__":
    Lactation_basics()