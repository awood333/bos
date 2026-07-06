'''
status_functions.wet_dry
'''
import inspect
from pathlib import Path
import pandas as pd
import numpy as np
from container import get_dependency

today = pd.Timestamp.today()

class WetDry:

        
    def __init__(self):
        print(f"WetDry instantiated by: {inspect.stack()[1].filename}")

        self.MB = None
        self.data = None
        self.ext_rng = None
        self.milk1 = None
        self.death_date = None
        self.datex = None
        self.start_pivot = None
        self.stop_pivot = None
        self.wy_id_list = None
        self.lacts = None
        self.startdate = None
        self.alive_ids = None

        self.wsd = None
        self.wmd = None
        self.milking_liters = None
        self.wet_days_df = None
        self.wet_days_array = None
        self.dry_days_array = None
        self.dry_days_df = None
        self.wdd_monthly = None
        self.wet_dry_df = None
        self.wet_dry_weekly = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.data = self.MB.data
        self.death_date = self.MB.bd[['wy_id','death_date']]
        self.ext_rng = self.MB.data['ext_rng']
        # Get fullday DataFrame from MilkAggregatesBasic and reindex to extended date range
        # Columns are integer indices from numpy; convert to strings to match str(wy_id) lookups
        MAB = get_dependency('milk_aggregates_basic')
        fullday = MAB.fullday.copy()
        fullday.columns = fullday.columns.astype(str)
        self.milk1 = fullday.reindex(self.MB.data['ext_rng'])
        self.datex = self.MB.data['datex']
        
        self.start_pivot = self.MB.data['lact_start_pivot']
        self.stop_pivot  = self.MB.data['lact_stop_pivot']
        
        
        self.wy_id_list = self.start_pivot.index
        self.lacts   = self.start_pivot.columns    
   
        
        
        DR = get_dependency('date_range')
        self.startdate = DR.startdate
        SD     = get_dependency('status_data')
        self.alive_ids = SD.alive_ids_last

        #methods
        self.wet_dry_2d     = self.create_wet_dry_2d()
        self.weekly_wetdry_table = self.create_wetdry_table
        
        
        
        
    def create_wet_dry_2d(self):
        idx = self.ext_rng
        wy_ids = self.MB.data['wy_ids']
        lacts = self.lacts
        lastday = self.MB.data['lastday']

        n_rows = len(idx)
        out = np.zeros((n_rows, len(wy_ids)))
        labels = np.full((n_rows, len(wy_ids)), '', dtype=object)          # >>> NEW

        for col, wy_id in enumerate(wy_ids):
            blocks = []
            label_blocks = []                                              # >>> NEW
            first_start = None
            prev_stop = None
            prev_lact = None                                                # >>> NEW

            for lact in lacts:
                start = (self.start_pivot.at[wy_id, lact]
                        if (wy_id in self.start_pivot.index and lact in self.start_pivot.columns)
                        else np.nan)
                stop  = (self.stop_pivot.at[wy_id, lact]
                        if (wy_id in self.stop_pivot.index and lact in self.stop_pivot.columns)
                        else np.nan)

                if pd.isna(start):
                    continue

                if first_start is None:
                    first_start = pd.Timestamp(start)  #useless but the bot likes it

                if prev_stop is not None:
                    dry_start = pd.Timestamp(prev_stop) + pd.Timedelta(days=1)
                    dry_end   = pd.Timestamp(start) - pd.Timedelta(days=1)
                    if dry_start <= dry_end:
                        n_dry = (dry_end - dry_start).days + 1
                        blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))
                        label_blocks.append(np.full((n_dry, 1), f'D{prev_lact}', dtype=object))   # >>> NEW

                wet_stop = lastday if pd.isna(stop) else pd.Timestamp(stop)
                if wet_stop < pd.Timestamp(start):
                    print(f"wy_id {wy_id}, lact {lact}: stop ({wet_stop.date()}) before start ({pd.Timestamp(start).date()}), skipped")
                    prev_stop = None if pd.isna(stop) else pd.Timestamp(stop)
                    prev_lact = lact
                    continue
                n_wet = (wet_stop - pd.Timestamp(start)).days + 1
                blocks.append(np.arange(1, n_wet + 1).reshape(-1, 1))
                label_blocks.append(np.full((n_wet, 1), f'W{lact}', dtype=object))

                prev_stop = None if pd.isna(stop) else pd.Timestamp(stop)
                prev_lact = lact                                            # >>> NEW

            # trailing dry period: cow's last lactation stopped, no later
            # lactation start exists in `lacts` -- she's currently open/dry
            if prev_stop is not None and prev_stop < lastday:               # >>> NEW
                dry_start = prev_stop + pd.Timedelta(days=1)                 # >>> NEW
                n_dry = (lastday - dry_start).days + 1                       # >>> NEW
                if n_dry > 0:                                                 # >>> NEW
                    blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))     # >>> NEW
                    label_blocks.append(np.full((n_dry, 1), f'D{prev_lact + 1}', dtype=object))  # >>> NEW

            if not blocks or first_start is None:
                continue

            stacked = np.vstack(blocks)
            stacked_labels = np.vstack(label_blocks)                        # >>> NEW

            try:
                row_offset = idx.get_loc(first_start)
            except KeyError:
                continue

            n = stacked.shape[0]
            rows_to_fill = min(n, n_rows - row_offset)
            out[row_offset:row_offset + rows_to_fill, col] = stacked[:rows_to_fill, 0]
            labels[row_offset:row_offset + rows_to_fill, col] = stacked_labels[:rows_to_fill, 0]  # >>> NEW

        self.wet_dry_2d = pd.DataFrame(out, index=idx, columns=wy_ids)
        self.period_2d = pd.DataFrame(labels, index=idx, columns=wy_ids)     # >>> NEW
        return self.wet_dry_2d
        
        def create_weekly_table(self, freq='W'):
            """
            Long-format weekly liters/revenue per wy_id, ready to write to Neon
            for the dashboard's plotting query. No 'period' column -- once you
            average across 7 days, a wet/dry label would just reflect whichever
            state dominated that week, which isn't a meaningful quantity, so
            it's dropped rather than carried through as noise.

            freq='W' for weekly, 'ME' for month-end (swap in if you want a
            monthly table alongside the weekly one -- same function, same shape).
            """
        if not hasattr(self, 'liters_2d'):
            self.create_liters_2d()

        # label='left', closed='left' pins the bin label to the Monday that
        # STARTS each week, rather than pandas' default (Sunday that ENDS
        # it) -- avoids the classic "why does this dip a week early" confusion
        # when the table is later plotted against a calendar axis.
        weekly = self.liters_2d.resample(freq, label='left', closed='left').mean()

        # wide (week_start x wy_id) -> long (week_start, wy_id, liters)
        long = weekly.stack().reset_index()
        long.columns = ['week_start', 'wy_id', 'liters']

        # revenue is a fixed multiple of liters (rate=22), so it can be
        # derived post-aggregation rather than needing its own 2D grid --
        # mean(liters)*22 == mean(liters*22), so this is exact, not an
        # approximation.
        long['revenue'] = long['liters'] * 22

        long = long.sort_values(['wy_id', 'week_start']).reset_index(drop=True)

        self.weekly_table = long
        return self.weekly_table


    def create_wetdry_table(self):
        """
        Long-format daily table for Neon: wy_id, date, day_num, liters.
        No period/revenue -- just wet_dry_2d (day_num) melted alongside
        liters_2d, joined on (date, wy_id). No concat with period_2d needed.
        """
        if not hasattr(self, 'liters_2d'):
            self.create_liters_2d()

        day_num_long = self.wet_dry_2d.stack().reset_index()
        day_num_long.columns = ['date', 'wy_id', 'day_num']

        liters_long = self.liters_2d.stack().reset_index()
        liters_long.columns = ['date', 'wy_id', 'liters']

        df = day_num_long.merge(liters_long, on=['date', 'wy_id'], how='left')
        df['liters'] = df['liters'].fillna(0)
        df = df.sort_values(['wy_id', 'date']).reset_index(drop=True)

        self.etdry_table = df
        return self.wetdry_table

if __name__ == '__main__':
    obj=WetDry()
    obj.load_and_process()      