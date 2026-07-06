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
        self.lastday = None
        self.datex = None
        self.extended_date_range_milk = None
        self.wy_id_list = None
        self.start = None
        self.stop = None
        self.lact_start_pivot = None
        self.lact_stop_pivot = None

    def load_and_process(self):
        self.data = self.dataLoader()

    def dataLoader(self):
        engine = get_engine()
        with engine.connect() as conn:
            
            self.stop   = pd.read_sql(text("SELECT * FROM stop_dates"),   conn)
            self.lb     = pd.read_sql(text("SELECT * FROM live_births"),  conn)
            self.u      = pd.read_sql(text("SELECT * FROM ultra"),        conn)
            self.i      = pd.read_sql(text("SELECT * FROM insem"),        conn)
            bd1         = pd.read_sql(text("SELECT * FROM birth_death"),  conn)
            # self.lb      = pd.read_sql(text("SELECT b_date FROM live_births"),  conn)
            
            self.lb     = self.lb   .drop(columns=['type'], errors='ignore')
            self.stop   = self.stop .drop(columns=['type'], errors='ignore')
            self.u      = self.u    .drop(columns=['type'], errors='ignore')
            self.i      = self.i    .drop(columns=['type'], errors='ignore')
            bd1         = bd1       .drop(columns=['type'], errors='ignore')
            

            # Get last date from am_wy view for lastday/datex
            am_wy_tmp = pd.read_sql(text('SELECT * FROM "AM_wy"'), conn)
            
        am_wy_tmp = am_wy_tmp.drop(columns=['type'], errors='ignore')
        am_wy_tmp = am_wy_tmp.set_index('date')
        
        # date cols — self.lb / self.stop
        self.lb['b_date']   = pd.to_datetime(self.lb['b_date'], errors='coerce')
        self.stop['stop']   = pd.to_datetime(self.stop['stop_date'],    errors='coerce')
        self.lb             = self.lb.fillna({'b_date': pd.NaT, 'calf_num': pd.NA})
        self.stop           = self.stop .fillna({'stop':   pd.NaT, 'calf_num': pd.NA})

        # date cols — bd1
        bd1['b_date']     = pd.to_datetime(bd1['b_date'])
        bd1['death_date'] = pd.to_datetime(bd1['death_date'], errors='coerce')
        bd1['arrived']    = pd.to_datetime(bd1['arrived'],    errors='coerce')
        bd1['adj_bdate']  = pd.to_datetime(bd1['adj_bdate'],  errors='coerce')

        # date cols — lb / u / i
        self.lb['b_date']    = pd.to_datetime(self.lb['b_date'],    errors='coerce')
        self.u['ultra_date'] = pd.to_datetime(self.u['ultra_date'], errors='coerce')
        self.i['insem_date'] = pd.to_datetime(self.i['insem_date'], errors='coerce')

        self.wy_id_list = bd1['wy_id'].tolist()

        # am_wy from Neon is long format (date, wy_ids, value)
        # Pivot to wide: index=date, columns=wy_ids
        am_wy_wide   = am_wy_tmp.T
        am_wy_wide.index.name = 'index'
        self.lastday = pd.to_datetime(am_wy_wide.columns[-1] , format='%Y-%m-%d', errors='coerce')
        self.datex   = pd.to_datetime(am_wy_wide.columns    , format='%Y-%m-%d', errors='coerce')
        # self.milk    = None  # populated by MilkAggregatesBasic

        print('lastday:  ', self.lastday)
        self.bd    = bd1

        start='2016-09-01'
        self.extended_date_range_milk = pd.date_range(
            start= start, end=self.lastday)




        lact_start1 = self.lb.loc[:,['wy_id', 'b_date', 'calf_num']]
        lact_start1['b_date'] = pd.to_datetime(lact_start1['b_date'], errors="coerce")
        
        self.lact_start_pivot = lact_start1.pivot_table(
            index   ='wy_id',
            columns ='calf_num',
            values  ='b_date',
            aggfunc ='first'  
        )
        
        lact_stop1 = self.stop.loc[:,['wy_id', 'stop_date', 'lact_num']]
        lact_stop1['stop_date'] = pd.to_datetime(lact_stop1['stop_date'], errors="coerce")        
        self.lact_stop_pivot = lact_stop1.pivot_table(
            index   ='wy_id',
            columns ='lact_num',
            values  ='stop_date',
            aggfunc ='first'  
        )



        self.data = {
            'datex'  : self.datex,
            'start'  : start,
            'stop'  : self.stop,
            'bd'     : self.bd,
            'lb'     : self.lb,
            'i'      : self.i,
            'u'      : self.u,
            'lastday': self.lastday,
            'wy_ids' : self.wy_id_list,
            'ext_rng': self.extended_date_range_milk,
            'lact_start_pivot': self.lact_start_pivot,
            'lact_stop_pivot': self.lact_stop_pivot
        }
        return self.data


if __name__ == '__main__':
    mb = MilkBasics()
    mb.load_and_process()