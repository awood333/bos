'''net_rev_by_cow.py'''
import inspect
import pandas as pd
from datetime import datetime
from container import get_dependency



class NetRev_byCow:
    def __init__(self):
        print(f"NetRev_byCow instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.DR = None
        self.FCT= None
        self.SD = None
        self.CPL = None

        self.today = None
        self.rng_monthly    = None
        self.rng_monthly2   = None
        self.rng_daily      = None
        self.alive_mask_str = None
        self.alive_mask_int = None



    def load_and_process(self):
        
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.FCT= get_dependency('feedcost_basics')
        self.SD = get_dependency('status_data')
        self.CPL= get_dependency('cow_pl')

        now = datetime.now()
        self.today = pd.to_datetime(now)

        self.rng_monthly  = self.DR.date_range_monthly
        self.rng_monthly2 = getattr(self.DR, 'date_range_monthly2', None)
        self.rng_daily    = self.DR.date_range_daily

        self.alive_mask_str = self.SD.alive_ids.to_list()
        self.alive_mask_int = self.SD.alive_ids.astype(int).to_list()

        self.df = self.get_groups_daily()
        self.get_rev_by_cow()

    def get_groups_daily(self):
        wetdays1 = self.CPL.wetdays.loc[self.rng_daily,:]


       
        
        return self.df

    def get_rev_by_cow(self):
        milk1 = self.MB.data['milk']
        milk2 = milk1.loc[self.rng_daily,self.alive_mask_str].copy()
        milk3 = milk2 * 22        
        x=1

#  self.df = pd.merge(wetdays1, milk2, left_index=True, right_index=True )


if __name__ == '__main__':
    obj = NetRev_byCow()
    obj.load_and_process()
