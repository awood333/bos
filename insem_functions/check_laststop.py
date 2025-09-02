'''check_laststop.py'''

# import pandas as pd
from MilkBasics                             import MilkBasics
from status_functions.statusData2           import StatusData2
from insem_functions.insem_ultra_basics       import InsemUltraBasics
from insem_functions.insem_ultra_data         import InsemUltraData



class CheckLastStop:
    def __init__(self, milk_basics=None, status_data2=None, insem_ultra_basics=None, insem_ultra_data=None):
            
        self.data = milk_basics or MilkBasics().data
        self.sd  = status_data2 or StatusData2()
        self.IUB = insem_ultra_basics or InsemUltraBasics()
        self.IUD = insem_ultra_data or InsemUltraData()
        
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
