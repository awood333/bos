'''check_laststop.py'''

import pandas as pd
from WetDryBasics import WetDryBasics
from status_2 import StatusData
from InsemUltraBasics import InsemUltraBasics
from InsemUltraFunctions import InsemUltraFunctions
from insem_ultra import InsemUltraData

bd = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date', 'death_date', 'adj_bdate'])

class CheckLastStop:
    def __init__(self):
            
        self.wdb = WetDryBasics()
        self.sd = StatusData()
        self.IUB = InsemUltraBasics()
        self.IUF = InsemUltraFunctions()
        self.IUD = InsemUltraData()
        
        self.allx = self.IUD.allx.iloc[:,:5].copy()   #first 5 cols of allx
        self.status_col = self.sd.status_col['status']
        self.last_stop = self.wdb.last_stop
        self.last_start = self.wdb.last_start
        self.cows = bd.index.tolist()
        
        self.last_stop.index = self.last_stop.index -1
        self.last_start.index = self.last_start.index -1

        self.listx = self.create_list()

# this is to identify missing last_stop dates. 
        # cows that are alive and milking but the laststop date/calf# is 

    def create_list(self):
        listx1 = []
        lstop = self.last_stop["last stop date"]
        lstart = self.last_start["last calf bdate"]
        for i in self.status_col.index.tolist():
            if i in self.last_stop.index and i in self.last_start.index:
                a = lstop[i]
                b = lstart[i]
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
