'''milk_basics.py'''
import inspect
import pandas as pd
from sqlalchemy import text
from sql_db_related.neon_connect import get_engine


class MilkBasics:
    def __init__(self):
        print(f"MilkBasics instantiated by: {inspect.stack()[1].filename}")
        self.data = None
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
        engine = get_engine()
        with engine.connect() as conn:
            startx = pd.read_sql(text("SELECT b_date FROM live_births"),  conn)
            stopx  = pd.read_sql(text("SELECT stop FROM stop_dates"),   conn)
            self.lb     = pd.read_sql(text("SELECT * FROM live_births"),  conn)
            self.u      = pd.read_sql(text("SELECT * FROM ultra"),        conn)
            self.i      = pd.read_sql(text("SELECT * FROM insem"),        conn)
            bd1         = pd.read_sql(text("SELECT * FROM birth_death"),  conn)

            # Get last date from am_wy view for lastday/datex
            am_wy_tmp = pd.read_sql(text('SELECT * FROM "AM_wy"'), conn)
            
        am_wy_tmp = am_wy_tmp.drop(columns=['type'], errors='ignore')
        am_wy_tmp = am_wy_tmp.set_index('date')
        
        # date cols — startx / stopx
        startx['b_date'] = pd.to_datetime(startx['b_date'], errors='coerce')
        stopx['stop']    = pd.to_datetime(stopx['stop'],    errors='coerce')
        startx = startx.fillna({'b_date': pd.NaT, 'calf_num': pd.NA})
        stopx  = stopx .fillna({'stop':   pd.NaT, 'calf_num': pd.NA})

        # date cols — bd1
        bd1['birth_date'] = pd.to_datetime(bd1['birth_date'])
        bd1['death_date'] = pd.to_datetime(bd1['death_date'], errors='coerce')
        bd1['arrived']    = pd.to_datetime(bd1['arrived'],    errors='coerce')
        bd1['adj_bdate']  = pd.to_datetime(bd1['adj_bdate'],  errors='coerce')

        # date cols — lb / u / i
        self.lb['b_date']    = pd.to_datetime(self.lb['b_date'],    errors='coerce')
        self.u['ultra_date'] = pd.to_datetime(self.u['ultra_date'], errors='coerce')
        self.i['insem_date'] = pd.to_datetime(self.i['insem_date'], errors='coerce')

        self.WY_ids = bd1['WY_id'].tolist()

        # am_wy from Neon is long format (date, wy_id, value)
        # Pivot to wide: index=date, columns=wy_id
        am_wy_wide   = am_wy_tmp.T
        am_wy_wide.index.name = 'index'
        self.lastday = pd.to_datetime(am_wy_wide.columns[-1] , format='%Y-%m-%d', errors='coerce')
        self.datex   = pd.to_datetime(am_wy_wide.columns    , format='%Y-%m-%d', errors='coerce')
        self.milk    = None  # populated by MilkAggregatesBasic

        print('lastday:  ', self.lastday)

        # pivot start/stop
        # start1a = startx.pivot_table(
        #     index='WY_id', columns='calf_num', values='b_date', fill_value=pd.NaT)
        # stop1a  = stopx.pivot_table(
        #     index='WY_id', columns='lact_num', values='stop',   fill_value=pd.NaT)

        # self.start = start1a.reindex(self.WY_ids).T
        # self.stop  = stop1a .reindex(self.WY_ids).T
        self.bd    = bd1

        self.extended_date_range_milk = pd.date_range(
            start='2016-09-01', end=self.lastday)

        self.data = {
            'milk'   : self.milk,
            'datex'  : self.datex,
            'start'  : self.start,
            'startx' : startx,
            'stop'   : self.stop,
            'stopx'  : stopx,
            'bd'     : self.bd,
            'lb'     : self.lb,
            'i'      : self.i,
            'u'      : self.u,
            'lastday': self.lastday,
            'WY_ids' : self.WY_ids,
            'ext_rng': self.extended_date_range_milk,
        }
        return self.data


if __name__ == '__main__':
    mb = MilkBasics()
    mb.load_and_process()