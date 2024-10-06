'''check_laststop.py'''

import pandas as pd
from milk_functions.WetDryBasics        import WetDryBasics
from milk_functions.status_2            import StatusData2
from insem_functions.InsemUltraBasics   import InsemUltraBasics
from insem_functions.InsemUltraFunctions import InsemUltraFunctions
from insem_functions.InsemUltraData        import InsemUltraData



class CheckLastStop:
    def __init__(self):
            
        self.wdb = WetDryBasics()
        self.sd  = StatusData2()
        self.IUB = InsemUltraBasics()
        self.IUF = InsemUltraFunctions()
        self.IUD = InsemUltraData()
        
        self.allx       = self.IUD.allx.iloc[:,:5].copy()   #first 5 cols of allx
        self.status_col = self.sd.status_col
        self.last_stop  = self.wdb.last_stop
        self.last_start = self.wdb.last_start
        
        bd          = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv')
        self.cows   = bd.index.tolist()
        
        self.last_stop.index  = self.last_stop.index -1
        self.last_start.index = self.last_start.index -1

        self.create_list()

# this is to identify missing last_stop dates. 
        # cows that are alive and milking but the laststop date/calf# is missing
        

    def create_list(self):
        listx = []
        status = self.status_col.reset_index()['ids']
        
        
        lstop = self.last_stop["last stop date"]
        lstart = self.last_start["last calf bdate"]
        for i in status.index.tolist():
            if i in self.last_stop.index and i in self.last_start.index:
                a = lstop[i]
                b = lstart[i]
                c = (status[i] == 'M')
                # d = (self.status_col != 'D')
              
                condition1 = a < b  # there is no birth after the last stop
                condition2 = c  # last status is 'M' 


                if condition1 and condition2:
                    listx.append(i)
            else:
                print(f"Index {i} not found in last_stop or last_start")
                
        if not listx:
            print('no laststop errors, listx is empty')
   

        return listx
