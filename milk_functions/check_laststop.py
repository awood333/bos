'''check_laststop.py'''

import pandas as pd
from MilkBasics                             import MilkBasics
from milk_functions.status_2                import StatusData2
from insem_functions.InsemUltraBasics       import InsemUltraBasics
from insem_functions.InsemUltraFunctions    import InsemUltraFunctions
from insem_functions.InsemUltraData         import InsemUltraData



class CheckLastStop:
    def __init__(self):
            
        self.data = MilkBasics().data
        self.sd  = StatusData2()
        self.IUB = InsemUltraBasics()
        self.IUF = InsemUltraFunctions()
        self.IUD = InsemUltraData()
        
        self.allx       = self.IUD.allx.iloc[:,:5].copy()   #first 5 cols of allx
        self.status_col = self.sd.status_col
        self.last_stop  = self.IUB.last_stop
        self.last_start = self.IUB.last_calf
        
        self.last_stop.index  = self.last_stop.index -1
        self.last_start.index = self.IUB.last_calf.index -1

        self.listx  = self.create_list()

# this is to identify missing last_stop dates. 
        # cows that are alive and milking but the laststop date/calf# is missing
        

    def create_list(self):
        self.listx = []
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
                    self.listx.append(i)
            else:
                print(f"Index {i} not found in last_stop or last_start")
                
        if not self.listx:
            print('no laststop errors, self.listx is empty')
   

        return self.listx
