'''lactations.py'''

import pandas as pd    #123   456
import numpy as np
from MilkBasics    import MilkBasics
from milk_functions.WetDry          import WetDry


class Lactation_basics:

    def __init__(self):

        self.data    = MilkBasics().data
        self.WD     = WetDry()
      
        self.lact4x, self.day_of_milking4 = self.create_lactation_basics()
        self.data = MilkBasics().data  
        
        
        
              
    def create_lactation_basics(self):

        milk = self.data.milk
        rows = list(self.data['stop2'].columns)  # these will be the headers in stop2, i.e., the lactations

        milk_cols1 = milk.columns
        cols = [int(x) for x in milk_cols1]

        # Initialize arrays to store the data
        max_rows = len(rows)
        max_cols = len(cols)
        max_depth = len(pd.date_range(start=self.data['start2'].min().min(), end=self.data['lastday']))

        lact4x = np.empty((max_rows, max_cols, max_depth), dtype=object)
        day_of_milking4 = np.empty((max_rows, max_cols, max_depth), dtype=object)

        for j, row in enumerate(milk.columns):
            for i, col in enumerate(rows):
                start = milk.loc[row[0], start]
                stop = milk.loc[row[0], stop]

                if pd.isna(start) and pd.isna(stop):  # start doesn't exist and stop doesn't exist - skip
                    continue

                date_range = pd.date_range(start=start, end=stop)

                # day_of_milking1 creates a series 1- len, which starts on the date in 'start' and ends on 'stop'
                day_of_milking1 = pd.Series(range(1, len(date_range) + 1), index=date_range, name=str(col))

                lact1 = milk.loc[start:stop, str(col)]

                day_of_milking4_list = []
                lact4x_list = []

                for k, date in enumerate(date_range):
                    day_of_milking4_list.append(day_of_milking1[date])
                    lact4x_list.append(lact1.iloc[k] if k < len(lact1) else np.nan)

                day_of_milking4[j, i, :len(day_of_milking4_list)] = np.hstack(day_of_milking4_list)
                lact4x[j, i, :len(lact4x_list)] = np.hstack(lact4x_list)

        self.lact4x = lact4x
        self.day_of_milking4 = day_of_milking4

        return self.lact4x, self.day_of_milking4

if __name__ == "__main__":
    Lactation_basics()