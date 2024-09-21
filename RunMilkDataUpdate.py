'''
RunMilkDailyUpdate.py
'''

from milkaggregates import MilkAggregates
from WetDryBasics import WetDryBasics
from WetDry import WetDry



def main():
    
    ma = MilkAggregates()
    wd = WetDry()
    
    
    ma.write_to_csv()
    wd.create_write_to_csv()


if __name__ == "__main__":
    main()
    