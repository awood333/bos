'''run_insem_status_IU.py'''

from milk_functions .status_2       import StatusData2
from insem_functions.insem_ultra    import InsemUltraData
from insem_functions.I_U_merge      import I_U_merge
from milk_functions .check_laststop import CheckLastStop

class Main():

    StatusData2()
    InsemUltraData()  # needs to be after status
    I_U_merge() 
    CheckLastStop()


if __name__ == "__main__":
    Main()
