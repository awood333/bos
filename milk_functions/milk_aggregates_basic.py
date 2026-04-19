'''milk_functions\\milk_aggregates_basic.py

Loads the 4 raw CSVs from Google Drive, computes AM/PM/fullday matrices,
and injects MB.data['milk'] with the freshly computed fullday.
No insem dependency — safe to load before status_data.
'''

from datetime import datetime
import sys
import os
import inspect
import pandas as pd
import numpy as np

from container import get_dependency
from utilities.gdrive_loader import gdrive_read_csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MilkAggregatesBasic:

    def __init__(self):
        print(f"MilkAggregatesBasic instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.data = None
        self.lag = -10
        self.date_format = '%m/%d/%Y'
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
        self.fullday_xl = None
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
         self.fullday_xl, self.fullday_lastdate] = self.fullday_calc()

        # Inject fresh fullday so all downstream consumers see current data
        self.MB.data['milk'] = self.fullday

    def basics(self):
        self.AM_liters = gdrive_read_csv("COWS/milk_data/raw/AM_liters.csv", index_col=0, header=0)
        self.AM_wy     = gdrive_read_csv("COWS/milk_data/raw/AM_wy.csv",     index_col=0, header=0)
        self.PM_liters = gdrive_read_csv("COWS/milk_data/raw/PM_liters.csv", index_col=0, header=0)
        self.PM_wy     = gdrive_read_csv("COWS/milk_data/raw/PM_wy.csv",     index_col=0, header=0)

        liters_am = self.AM_liters
        wy_am     = self.AM_wy
        liters_pm = self.PM_liters
        wy_pm     = self.PM_wy

        self.datex = pd.to_datetime(self.AM_liters.columns, errors="coerce")
        last_index_value = self.datex[-1]
        print('last index value ', last_index_value)

        self.maxcols = len(self.datex)
        maxrows      = len(self.data['bd']['WY_id'])

        idx          = np.zeros((maxrows + 1, self.maxcols), dtype=int)
        self.idx_am  = idx.copy()
        self.idx_pm  = idx.copy()

        self.wy_am_np     = wy_am.to_numpy(dtype=float)
        self.wy_pm_np     = wy_pm.to_numpy(dtype=float)
        self.liters_am_np = liters_am.to_numpy(dtype=float)
        self.liters_pm_np = liters_pm.to_numpy(dtype=float)

        return [self.maxcols, self.idx_am, self.idx_pm,
                self.wy_am_np, self.wy_pm_np,
                self.liters_am_np, self.liters_pm_np]

    def fullday_calc(self):
        # AM calc
        target_am = []
        i = 0
        while i < self.maxcols:
            index1  = self.wy_am_np[:, i]
            index2  = np.nan_to_num(index1, nan=0).astype(int)
            value1  = self.liters_am_np[:, i]
            value2  = np.nan_to_num(value1, nan=0.0).astype(float)
            target1 = self.idx_am[:, i].astype(float)
            target1[index2] = value2
            target_am.append(target1)
            i += 1
        am1 = pd.DataFrame(target_am)

        self.am = am1.T
        self.am.columns = self.AM_liters.columns
        self.am.replace(0, np.nan, inplace=True)
        self.am.drop(self.am.columns[0], axis=1, inplace=True)

        # PM calc
        target_pm = []
        i = 0
        while i < self.maxcols:
            index1  = self.wy_pm_np[:, i]
            index2  = np.nan_to_num(index1, nan=0).astype(int)
            value1  = self.liters_pm_np[:, i]
            value2  = np.nan_to_num(value1, nan=0.0).astype(float)
            target1 = self.idx_pm[:, i].astype(float)
            target1[index2] = value2
            target_pm.append(target1)
            i += 1
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
        self.fullday.index = pd.to_datetime(self.fullday.index, errors='coerce', format="%m/%d/%Y").date
        self.fullday.replace(0, np.nan, inplace=True)
        self.fullday.drop(self.fullday.iloc[:, 0:1], axis=1, inplace=True)
        self.fullday.index.name = 'datex'

        self.fullday_xl = self.fullday.copy()
        self.fullday_xl.index = self.fullday_xl.index.map(
            lambda x: (x - datetime(1899, 12, 30).date()).days)
        self.fullday_lastdate = pd.DataFrame(
            index=[self.fullday.index[-1]], columns=['last_date'])

        return [self.am, self.pm, self.fullday, self.fullday_xl, self.fullday_lastdate]


if __name__ == '__main__':
    obj = MilkAggregatesBasic()
    obj.load_and_process()
