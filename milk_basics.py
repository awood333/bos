'''milk_basics.py'''
import inspect
import pandas as pd
from utilities.gdrive_loader import gdrive_read_csv


class MilkBasics:
    def __init__(self):
        print(f"MilkBasics instantiated by: {inspect.stack()[1].filename}")
        self.data = None
        self.startx = None
        self.stopx = None
        self.lb = None
        self.u = None
        self.i = None
        self.bd = None
        self.milk = None
        self.lastday = None
        self.datex = None
        self.extended_date_range_milk = None
        self.WY_ids = None
        self.start = None
        self.stop = None

    def load_and_process(self):
        self.data = self.dataLoader()
        
        
    def dataLoader(self):
         
        self.startx  = gdrive_read_csv("COWS/basic_data/live_births.csv", index_col=None,  header=0)
        self.stopx   = gdrive_read_csv("COWS/basic_data/stop_dates.csv",  index_col=None,  header=0)
        
        # date cols
        self.startx   ['b_date']        = pd.to_datetime(self.startx['b_date'], errors='coerce')    
        self.stopx    ['stop']          = pd.to_datetime(self.stopx['stop'], errors='coerce')
        
        self.startx = self.startx.fillna({'b_date': pd.NaT, 'calf#': pd.NA})
        self.stopx = self.stopx.fillna({'stop': pd.NaT, 'calf#': pd.NA})
         
        bd1      = gdrive_read_csv("COWS/basic_data/birth_death.csv",  header=0)
        self.lb  = gdrive_read_csv("COWS/basic_data/live_births.csv",  index_col=None)
        self.u   = gdrive_read_csv("COWS/basic_data/ultra.csv")
        self.i   = gdrive_read_csv("COWS/basic_data/insem.csv") 
        
        bd1['birth_date']      = pd.to_datetime(bd1['birth_date'])
        bd1['death_date']      = pd.to_datetime(bd1['death_date'], errors='coerce')        
        bd1['arrived']         = pd.to_datetime(bd1['arrived'], errors='coerce')
        bd1['adj_bdate']       = pd.to_datetime(bd1['adj_bdate'], errors='coerce')
        
        self.lb['b_date']         = pd.to_datetime (self.lb    ['b_date'], errors='coerce')
        self.u ['ultra_date']     = pd.to_datetime (self.u     ['ultra_date'], errors='coerce')
        self.i ['insem_date']     = pd.to_datetime (self.i     ['insem_date'], errors='coerce')
        
        start1a  = self.startx.pivot_table (index='WY_id', columns='calf#',    values='b_date', fill_value=pd.NaT)
        stop1a   = self.stopx .pivot_table (index='WY_id', columns='lact_num', values='stop',   fill_value=pd.NaT)
     
        start2a = start1a.reindex(self.WY_ids)
        stop2a  = stop1a  .reindex(self.WY_ids)
                
        self.milk       = gdrive_read_csv("COWS/milk_data/fullday/fullday.csv", header=0, index_col='datex')
        self.milk.index = pd.to_datetime(self.milk.index)

        # Derive lastday from AM_wy (live Google Sheet)
        am_wy_tmp = gdrive_read_csv("COWS/milk_data/raw/AM_wy.csv", index_col=0, header=0)
        self.lastday = pd.to_datetime(am_wy_tmp.columns[-1], errors='coerce')
        self.datex = self.milk.index

        # NOTE: extended range is renamed ext_rng in 'data'
        self.extended_date_range_milk = pd.date_range(start='2016-09-01', end=self.lastday)
      
        self.WY_ids = bd1['WY_id'].tolist()

        
        self.start = start2a.T
        self.stop  = stop2a.T
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
            'WY_ids'    : self.WY_ids,
            'ext_rng'   : self.extended_date_range_milk
                }
        
        return self.data
        


if __name__ == '__main__':
    mb=MilkBasics()
    mb.load_and_process()