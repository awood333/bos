'''WetDryBasics.py'''
import pandas as pd
import numpy as np

from insem_functions.InsemUltraBasics import InsemUltraBasics

class WetDryBasics:
    def __init__(self):
        
        self.IUB = InsemUltraBasics()
        
        self.milk,  self.start2, self.stop2, self.lastday   = self.dataLoader()
        
    def dataLoader(self):

        stopx    = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  parse_dates=['stop'], header=0)
        bd1      = pd.read_csv ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], header=0, index_col='WY_id')
        startx   = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'],header=0)
        milk1a    = pd.read_csv  ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', parse_dates=['datex'], header=0, index_col=0)

        start1a  = startx.pivot (index='WY_id', columns='calf#',    values='b_date')          
        stop1a   = stopx .pivot (index='WY_id', columns='lact_num', values='stop')

        cutoff1 = 90  
        cutoff2 = 95
        cutoff3 = None  
        
        self.milk   = milk1a.iloc[cutoff3:,cutoff1:cutoff2].copy()

        extended_rng_milk = pd.date_range(start='9/1/2016', end= self.milk.index[-1])
      
        rng = bd1.index.tolist()
        start2a = start1a.reindex(rng)
        stop2a  = stop1a .reindex(rng)
        
        start2b = start2a.iloc[cutoff1:cutoff2,:].copy()    
        stop2b  = stop2a .iloc[cutoff1:cutoff2,:].copy()

        self.start2 = start2b
        self.stop2  = stop2b
        
        self.last_start = self.IUB.last_calf
        self.last_stop  = self.IUB.last_stop
        self.lastday= self.milk.index[-1]
        # dd = bd1['death_date']
        
        return self.milk,  self.start2, self.stop2, self.lastday
        
        


if __name__ == '__main__':
    WetDryBasics()