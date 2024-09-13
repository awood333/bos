'''check_laststop.py'''

import pandas as pd
from wet_dry import WetDry
from status_2 import StatusData
from insem_ultra import InsemUltraData

bd = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date', 'death_date', 'adj_bdate'])

class CheckLastStop:
    def __init__(self):
            
        wd = WetDry()
        sd = StatusData()
        iu = InsemUltraData()
        
        self.allx = iu.allx.iloc[:,:5].copy()   #first 5 cols of allx
        self.status_col = sd.status_col['status']
        self.last_stop = wd.max_stop
        self.last_start = wd.max_start
        self.cows = bd.index.tolist()
        
        self.last_stop.index = self.last_stop.index -1
        self.last_start.index = self.last_start.index -1

        self.listx = self.create_list()

# this is to identify missing last_stop dates. 
        # cows that are alive and milking but the laststop date/calf# is 

    def create_list(self):
        listx1 = []
        for i in self.status_col.index.tolist():
            if i in self.last_stop.index and i in self.last_start.index:
                a = self.last_stop[i]
                b = self.last_start[i]
                c = (self.status_col != 'M')
                d = (self.status_col != 'D')
              
                condition1 = a < b  # there is a birth after the last stop
                condition2 = ~(c & d)  # last status is neither 'M' nor 'D'-- not milking

                # meaning, if a calf was born, but the status isn't 'milking', 
                # probably there is no stop-date entered (even if they're dead)

                if condition1 and condition2.any():
                    listx1.append(i)
            else:
                print(f"Index {i} not found in last_stop or last_start")
                
        listx2 = [x+1 for x in listx1]  
        listx3  =  pd.DataFrame(listx2, columns=['WY_id'])  
        listx3.set_index('WY_id', inplace=True)
        
        listx = pd.merge(self.allx, listx3, how='right', on='WY_id')

        return listx



    def write_to_csv(self):
        self.listx.to_csv(('F:\\COWS\\data\\status\\check_list.csv'))
