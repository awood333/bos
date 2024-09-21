'''run_insem_status_IU.py'''

from status_2 import StatusData
from insem_ultra import InsemUltraData
from I_U_merge import I_U_merge
from check_laststop import CheckLastStop

def main():

    sd = StatusData()
    sd.create_write_to_csv()

    iud = InsemUltraData()  # needs to be after status
    iud.create_write_to_csv()
 
    ium = I_U_merge() 
    ium.create_write_to_csv()
 
    chls = CheckLastStop()
    chls.write_to_csv()

if __name__ == "__main__":
    main()
