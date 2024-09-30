'''run_milk_functs.py'''

from milk_functions.lactation1  import ThisLactation
from milk_functions.lactations2 import WeeklyLactations
from milk_functions.WetDry      import WetDry
from milk_functions.check_laststop import CheckLastStop

def Main():
    
    WetDry()
    WeeklyLactations()
    ThisLactation()
    CheckLastStop()


if __name__ == "__main__":
    Main()
    