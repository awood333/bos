'''run_insem_status_IU.py'''

from milk_functions .status_2               import StatusData2
from milk_functions .status_ids             import StatusData

from insem_functions.InsemUltraBasics       import InsemUltraBasics
from insem_functions.InsemUltraFunctions    import InsemUltraFunctions
from insem_functions.InsemUltraData         import InsemUltraData

from insem_functions.I_U_merge              import I_U_merge
from milk_functions .check_laststop         import CheckLastStop

class Main():
    def __init__ (self):

        StatusData2()
        StatusData()  # needs to be after status
        self.insem_ultra_data = InsemUltraData()
        self.insem_ultra_basics = InsemUltraBasics()
        self.insem_ultra_functions = InsemUltraFunctions()
        self.i_u_merge = I_U_merge()
        self.check_last_stop = CheckLastStop()


if __name__ == "__main__":
    Main()
