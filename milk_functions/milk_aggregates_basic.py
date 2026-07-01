'''milk_functions/milk_aggregates_basic.py

Loads milk data from Neon (milk_transpose table), computes AM/PM/fullday matrices,
and injects MB.data['milk'] with the freshly computed fullday.
No insem dependency — safe to load before status_data.
'''

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import inspect
import pandas as pd
import numpy as np
from sqlalchemy import text

from container import get_dependency
from sql_db_related.neon_connect import get_engine


class MilkAggregatesBasic:

    def __init__(self):
        print(f"MilkAggregatesBasic instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.data = None
        self.lag = -10
        self.maxcols = None
        self.idx_am = None
        self.idx_pm = None
        self.wy_am_np = None
        self.wy_pm_np = None
        self.liters_am_np = None
        self.liters_pm_np = None
        self.am = None
        self.pm = None
        self.fullday = None
        self.fullday_lastdate = None
        self.AM_liters = None
        self.AM_wy = None
        self.PM_liters = None
        self.PM_wy = None
        self.datex = None

    def load_and_process(self):
        self.MB   = get_dependency('milk_basics')
        self.data = self.MB.data

        [self.maxcols, self.idx_am, self.idx_pm,
         self.wy_am_np, self.wy_pm_np,
         self.liters_am_np, self.liters_pm_np] = self.basics()

        [self.am, self.pm, self.fullday,
         self.fullday_lastdate] = self.fullday_calc()

        self.MB.data['milk'] = self.fullday

    def basics(self):
        # Read milk_transpose from Neon — four targeted queries
        engine = get_engine()
        with engine.connect() as conn:
            def _read_type(type_name):
                sql = f"SELECT * FROM milk_transpose WHERE type = '{type_name}'"
                return (pd.read_sql(sql, conn)
                          .drop(columns=['type'])
                          .set_index('date')
                          .T)

            self.AM_liters = _read_type('AM_liters')
            self.AM_wy     = _read_type('AM_wy')
            self.PM_liters = _read_type('PM_liters')
            self.PM_wy     = _read_type('PM_wy')

        self.datex = pd.to_datetime(self.AM_liters.columns, errors="coerce")
        print('last index value ', self.datex[-1])

        self.maxcols = len(self.datex)
        maxrows      = len(self.data['bd']['wy_id'])

        idx          = np.zeros((maxrows + 1, self.maxcols), dtype=int)
        self.idx_am  = idx.copy()
        self.idx_pm  = idx.copy()

        self.wy_am_np     = self.AM_wy    .to_numpy(dtype=float)
        self.wy_pm_np     = self.PM_wy    .to_numpy(dtype=float)
        self.liters_am_np = self.AM_liters.to_numpy(dtype=float)
        self.liters_pm_np = self.PM_liters.to_numpy(dtype=float)

        return [self.maxcols, self.idx_am, self.idx_pm,
                self.wy_am_np, self.wy_pm_np,
                self.liters_am_np, self.liters_pm_np]

    def fullday_calc(self):
        # AM calc
        target_am = []
        for i in range(self.maxcols):
            index2  = np.nan_to_num(self.wy_am_np[:, i],     nan=0).astype(int)
            value2  = np.nan_to_num(self.liters_am_np[:, i], nan=0.0).astype(float)
            target1 = self.idx_am[:, i].astype(float)
            target1[index2] = value2
            target_am.append(target1)
        am1 = pd.DataFrame(target_am)

        self.am = am1.T
        self.am.columns = self.AM_liters.columns
        self.am.replace(0, np.nan, inplace=True)
        self.am.drop(self.am.columns[0], axis=1, inplace=True)

        # PM calc
        target_pm = []
        for i in range(self.maxcols):
            index2  = np.nan_to_num(self.wy_pm_np[:, i],     nan=0).astype(int)
            value2  = np.nan_to_num(self.liters_pm_np[:, i], nan=0.0).astype(float)
            target1 = self.idx_pm[:, i].astype(float)
            target1[index2] = value2
            target_pm.append(target1)
        pm1 = pd.DataFrame(target_pm)

        self.pm = pm1.T
        self.pm.columns = self.datex
        self.pm.replace(0, np.nan, inplace=True)
        self.pm.drop(self.pm.columns[0], axis=1, inplace=True)

        # fullday calc
        fullday1 = np.add(am1, pm1)
        fullday2 = pd.DataFrame(fullday1)
        fullday2['datex'] = self.datex
        fullday2.set_index('datex', inplace=True)

        self.fullday = fullday2
        self.fullday.index = pd.to_datetime(self.fullday.index, errors='coerce')  # ISO format from Neon
        self.fullday.replace(0, np.nan, inplace=True)
        self.fullday.drop(self.fullday.iloc[:, 0:1], axis=1, inplace=True)
        self.fullday.index.name = 'datex'

        self.fullday_lastdate = pd.DataFrame(
            index=[self.fullday.index[-1]], columns=['last_date'])

        return [self.am, self.pm, self.fullday, self.fullday_lastdate]


if __name__ == '__main__':
    obj = MilkAggregatesBasic()
    obj.load_and_process()