'''run_milk_functs.py'''

# from lactations import Lactations
# from lactations2 import WeeklyLactations
from wet_dry_2 import WetDry2

def main():
    
    
    # lact = Lactations()

    # lact2 = WeeklyLactations()

    wd = WetDry2()
    wd.create_write_to_csv()


if __name__ == "__main__":
    main()
    