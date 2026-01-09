'''
status_functions.status_data
'''
import inspect
import pandas as pd
from container import get_dependency
# import math
# import json

class status_data:
    def __init__(self):
        print(f"status_data instantiated by: {inspect.stack()[1].filename}")
        # Data dependencies
        self.MB = None
        self.DR = None
        self.startdate = None
        self.enddate_daily = None
        self.f = None
        self.maxdate = None
        self.stopdate = None
        self.bd1 = None
        self.bdmax = None
        self.wy_series = None
        self.milker_ids = None
        self.dry_ids = None
        self.alive_ids = None
        self.gone_ids = None
        self.milkers_ids = None
        self.dry_ids_last = None
        self.alive_count = None
        self.gone_count = None
        self.milker_count = None
        self.dry_count = None
        self.milker_ids_df = None
        self.dry_ids_df = None
        self.herd_daily = None
        self.herd_monthly = None
        self.status_col = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        self.startdate = self.DR.startdate
        self.enddate_daily = getattr(self.DR, 'enddate_daily', None)
        f1 = self.MB.data['milk']
        self.f = f1.loc[self.startdate:, :].copy()
        self.maxdate = self.f.index.max()
        self.stopdate = self.maxdate
        self.bd1 = self.MB.data['bd']
        self.bdmax = len(self.bd1)
        self.wy_series = pd.Series(list(range(1, self.bdmax + 1)), name='WY_id', index=range(1, self.bdmax + 1))
        # Per-date lists
        [self.milker_ids, self.dry_ids, self.alive_ids,
         self.alive_count, self.milker_count, self.dry_count] = self.create_milking_list()
        self.milker_ids_df, self.dry_ids_df = self.create_df()
        self.herd_daily = self.create_herd_daily()
        self.herd_monthly = self.create_herd_monthly()
        # Simple snapshot (last day)
        self.create_status_col()
        self.create_write_to_csv()

    def create_milking_list(self):
        # Prepare lists to collect per-date data
        milker_ids_list, dry_ids_list, alive_ids_list = [], [], []
        alive_count, milker_count, dry_count = [], [], []
        datex = self.f.index
        bd = self.bd1['birth_date']
        bd.index += 1
        bd.index = bd.index.astype(str)
        dd = self.bd1['death_date']
        dd.index += 1
        dd.index = dd.index.astype(str)
        for date in datex:
            alive_mask = []
            for i in self.f.columns:
                alive1 = date > bd.loc[i] and (date < dd.loc[i] or pd.isna(dd.loc[i]))
                alive_mask.append(alive1)
            alive_series = self.f.loc[date, alive_mask].copy()
            milking_mask = alive_series > 0
            dry_mask = pd.isna(alive_series)
            milker1 = list(alive_series.index[milking_mask])
            dry1 = list(alive_series.index[dry_mask])
            alive1 = list(alive_series.index)
            milker_ids_list.append(milker1)
            dry_ids_list.append(dry1)
            alive_ids_list.append(alive1)
            alive_count.append(len(alive1))
            milker_count.append(len(milker1))
            dry_count.append(len(dry1))
        # Convert lists of lists to DataFrames, padding with None for unequal lengths
        milker_ids = pd.DataFrame(milker_ids_list, index=datex)
        dry_ids = pd.DataFrame(dry_ids_list, index=datex)
        alive_ids = pd.DataFrame(alive_ids_list, index=datex)
        return [milker_ids, dry_ids, alive_ids, alive_count, milker_count, dry_count]

    def create_df(self):
        df = pd.DataFrame(self.milker_ids)
        milker_ids_df = df.set_index(self.f.index)
        df2 = pd.DataFrame(self.dry_ids)
        dry_ids_df = df2.set_index(self.f.index)
        return milker_ids_df, dry_ids_df

    def create_herd_daily(self):
        data = {
            'alive': self.alive_count,
            'milkers': self.milker_count,
            'dry': self.dry_count
        }
        herd1 = pd.DataFrame(data, index=self.f.index)
        herd1['dry_15pct'] = (herd1['milkers'] * .15).to_frame(name='dry 15pct')
        self.herd_daily = herd1
        return self.herd_daily

    def create_herd_monthly(self):
        hm = self.herd_daily.groupby(pd.Grouper(freq='ME')).mean()
        hm['year'] = hm.index.year
        hm['month'] = hm.index.month
        hm.set_index(['year', 'month'], inplace=True)
        self.herd_monthly = hm
        return self.herd_monthly

    def create_status_col(self):
        # All snapshot logic handled here
        bd = self.MB.data['bd'].reset_index(drop=True)
        alive_mask = bd['death_date'].isnull()
        gone_mask = bd['death_date'].notnull()
        alive_ids_last = bd.loc[alive_mask, 'WY_id'].to_list()
        gone_ids = bd.loc[gone_mask, 'WY_id'].to_list()
        last_milking = self.MB.data['milk'].iloc[-1:, :]
        milkers_mask = self.MB.data['milk'].iloc[-1, :] > 0
        milkers_ids = last_milking.columns[milkers_mask].astype(int).tolist()
        dry_ids_last = [id for id in alive_ids_last if id not in milkers_ids]
        dry_last = pd.DataFrame(dry_ids_last, columns=['ids']).reset_index(drop=True)
        dry_last['status'] = 'D'
        milkers_last = pd.DataFrame(milkers_ids, columns=['ids']).reset_index(drop=True)
        milkers_last['status'] = 'M'
        goners_last = pd.DataFrame(gone_ids, columns=['ids']).reset_index(drop=True)
        goners_last['status'] = 'G'
        alive_count_last = len(alive_ids_last)
        gone_count = len(gone_ids)
        milkers_count_last = len(milkers_ids)
        dry_count_last = len(dry_ids_last)

        # Store as self attributes if needed elsewhere
        self.alive_ids_last = alive_ids_last
        self.gone_ids = gone_ids
        self.milkers_ids = milkers_ids
        self.dry_ids_last = dry_ids_last
        self.dry_last = dry_last
        self.milkers_last = milkers_last
        self.goners_last = goners_last
        self.alive_count_last = alive_count_last
        self.gone_count = gone_count
        self.milkers_count_last = milkers_count_last
        self.dry_count_last = dry_count_last

        # Build status_col
        status_col1 = pd.concat([milkers_last, goners_last, dry_last], axis=0)
        status_col2 = status_col1.sort_values(by='ids')
        status_col3 = status_col2.reset_index(drop=True)
        self.status_col = status_col3
        return self.status_col

    def create_write_to_csv(self):
        # Per-date (status_data)
        self.milker_ids_df.to_csv('F:\\COWS\\data\\status\\milker_ids.csv')
        self.dry_ids_df.to_csv('F:\\COWS\\data\\status\\dry_ids.csv')
        self.herd_daily.to_csv('F:\\COWS\\data\\status\\herd_daily.csv')
        self.herd_monthly.to_csv('F:\\COWS\\data\\status\\herd_monthly.csv')
        # Snapshot (status_data2)
        pd.DataFrame(self.milkers_ids, columns=['ids']).to_csv('F:\\COWS\\data\\status\\milkers_ids_last.csv')
        pd.DataFrame(self.dry_ids_last, columns=['ids']).to_csv('F:\\COWS\\data\\status\\dry_ids_last.csv')
        self.status_col.to_csv('F:\\COWS\\data\\status\\status_col.csv')

if __name__ == "__main__":
    obj = status_data()
    obj.load_and_process()
