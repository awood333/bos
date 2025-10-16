'''check_laststop.py'''
import inspect
from container import get_dependency
from milk_basics import MilkBasics
from date_range import DateRange

class CheckLastStop:
    def __init__(self):
        print(f"CheckLastStop instantiated by: {inspect.stack()[1].filename}")
        print(f"🔍 {self.__class__.__module__}: Current stack:")
        for i, frame in enumerate(inspect.stack()[:5]):
            print(f"   {i}: {frame.filename}:{frame.lineno} in {frame.function}")        
        
        self.MB = MilkBasics()
        self.DR = DateRange()
        
    
        self.data = self.MB.data
        self.sd = get_dependency('status_data2')
        self.IUB = get_dependency('insem_ultra_basics')
        self.IUD = get_dependency('insem_ultra_data')
 
        self.allx = self.IUD.allx.iloc[:,:5].copy()   # first 5 cols of allx
        self.status_col = self.sd.status_col
        self.last_stop = self.IUB.last_stop
        self.last_start = self.IUB.last_calf
        
        self.last_stop.index = self.last_stop.index - 1
        self.last_start.index = self.IUB.last_calf.index - 1

        self.listx = self.create_list()
        print("✅ CheckLastStop: Complete!")

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
