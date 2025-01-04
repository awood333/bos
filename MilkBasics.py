'''MilkBasics.py'''
import pandas as pd
from typing import Dict, Any


class MilkBasics:
    def __init__(self):
        
        self.data: Dict[str, Any]      = self.dataLoader()
        
        
    def dataLoader(self):
         
        self.startx  = pd.read_csv    ('F:\\COWS\\data\\csv_files\\live_births.csv', index_col=None,     header = 0)
        self.stopx   = pd.read_csv    ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  index_col=None,     header = 0)
        
        # date cols
        self.startx   ['b_date']        = pd.to_datetime(self.startx['b_date'])    
        self.stopx    ['stop']          = pd.to_datetime(self.stopx['stop'])

        
        
        self.startx = self.startx.fillna({'b_date': pd.NaT, 'calf#': pd.NA})
        self.stopx = self.stopx.fillna({'stop': pd.NaT, 'calf#': pd.NA})
        
        
        milk1a       = pd.read_csv    ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', header = 0, index_col='datex')
        milk1a.index                    = pd.to_datetime(milk1a.index)
        

        bd1      = pd.read_csv    ('F:\\COWS\\data\\csv_files\\birth_death.csv',     header = 0)
        self.lb  = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv', index_col=None) 
        self.u   = pd.read_csv('F:\\COWS\\data\\csv_files\\ultra.csv')
        self.i   = pd.read_csv('F:\\COWS\\data\\csv_files\\insem.csv') 




        # date cols
        
        # self.startx   ['b_date']        = pd.to_datetime(self.startx['b_date'])    
        # self.stopx    ['stop']          = pd.to_datetime(self.stopx['stop'])
        # milk1a.index                    = pd.to_datetime(milk1a.index)
        
        bd1         ['birth_date']      = pd.to_datetime(bd1['birth_date'])
        bd1         ['death_date']      = pd.to_datetime(bd1['death_date'], errors='coerce')        
        bd1         ['arrived']         = pd.to_datetime(bd1['arrived'], errors='coerce')
        bd1         ['adj_bdate']       = pd.to_datetime(bd1['adj_bdate'], errors='coerce')
        
        self.lb      ['b_date']         = pd.to_datetime (self.lb    ['b_date'], errors='coerce')
        self.u       ['ultra_date']     = pd.to_datetime (self.u     ['ultra_date'], errors='coerce')
        self.i       ['insem_date']     = pd.to_datetime (self.i     ['insem_date'], errors='coerce')
        

        start1a  = self.startx.pivot_table (index='WY_id', columns='calf#',    values='b_date', fill_value=pd.NaT)
        stop1a   = self.stopx .pivot_table (index='WY_id', columns='lact_num', values='stop',   fill_value=pd.NaT)
   
        cutoff1 = None
        cutoff2 = None
        cutoff3 = None
        
        
        self.milk   = milk1a.loc[cutoff3:, cutoff1:cutoff2].copy()
        self.lastday = self.milk.index[-1]
        self.datex = self.milk.index

        # Note extended range is renamed ext_rng in 'data'
        self.extended_date_range_milk = pd.date_range(start='2016-09-01', end= self.milk.index[-1])
      
        self.rng = bd1['WY_id'].tolist()
     
        start2a = start1a.reindex(self.rng)
        stop2a  = stop1a  .reindex(self.rng)
        
        # # need to increment the index to match the cutoffs in 'milk'
        start2b = start2a.loc[ cutoff1:cutoff2, : ].copy()    
        stop2b  = stop2a .loc[ cutoff1:cutoff2, : ].copy()

        self.start = start2b.T
        self.stop  = stop2b.T
        self.bd = bd1

        
        
        self.data = {
            'milk'      : self.milk,
            'datex'     : self.datex,
            'start'     : self.start,
            'startx'    : self.startx, 
            'stop'      : self.stop,
            'stopx'     : self.stopx,   #this is the raw stop_dates file
            'bd'        : self.bd,
            'lb'        : self.lb,
            'i'         : self.i,
            'u'         : self.u,
            'lastday'   : self.lastday,
            'rng'       : self.rng,
            'ext_rng'   : self.extended_date_range_milk
                }
        
        return self.data
        


if __name__ == '__main__':
    MilkBasics()