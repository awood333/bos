'''
status_functions.wet_dry
'''
import inspect
import pandas as pd
import json
from container import get_dependency

today = pd.Timestamp.today()

class WetDry:

        
    def __init__(self):
        print(f"WetDry instantiated by: {inspect.stack()[1].filename}")

        self.MB = None
        self.data = None
        self.ext_rng = None
        self.milk1 = None
        self.datex = None
        self.WY_ids = None

        self.wsd = None
        self.wmd = None
        self.milking_liters = None
        self.wet_days_df = None
        self.dry_days_df = None
        self.wdd_monthly = None
        self.wet_dry_dict = None
        self.wet_dry_df = None
        self.wet_dry_weekly = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.data = self.MB.data
        self.ext_rng = self.MB.data['ext_rng']
        self.milk1 = self.MB.data['milk']
        self.datex = self.MB.data['datex']
        self.WY_ids = self.MB.data['start'].columns

        [self.wet_days_df, self.wsd, 
         self.wmd, self.milking_liters] = self.create_wet_days()

        self.dry_days_df = self.create_dry_days()

        self.wet_dry_dict = self.create_wet_dry_dict()
        self.wet_dry_df   = self.flatten_dict_save_as_df()
        self.wet_dry_weekly = self.create_wetdry_weekly()
        self.write_to_csv_json()


    def create_wet_days(self):

        wet_days1 = wet_days2 = wet_days3 = pd.DataFrame()
        wet_sum_df1 = wet_sum2 = wet_sum3 = pd.DataFrame()
        wet_max_df1 = wet_max2 = wet_max3 = pd.DataFrame()
        milking_liters1 = milking_liters2 = pd.DataFrame()        
        idx     = self.ext_rng
        WY_ids  = self.MB.data['WY_ids']   #NOTE:put a 1 to sliceWY_ids = WY_ids1[250:254]

        i=0
        
        lacts   = self.MB.data['stop'].index      # lact# (float)

        for i in WY_ids:
            for j in lacts:

                lastday = self.MB.data['lastday']  # last day of the milk df datex
                start = self.MB.data ['start'].loc[j, i]
                stop  = self.MB.data ['stop'] .loc[j, i]

                a = pd.isna(start)  is False  # start value exists
                b = pd.isna(stop)   is False  # stop value exists
                e = pd.isna(start)  is True   # start value missing
                f = pd.isna(stop)   is True   # stop value missing

                # completed lactation:
                if a and b:
                    days_range = pd.date_range(start, stop)
                    day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                    wet_days1 = pd.DataFrame(day_nums, days_range)
                
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    wet_sum1 = pd.DataFrame([wet1a.sum()], columns=[j], index=[i])
                    wet_max1 = pd.DataFrame([wet1a.max()], columns=[j], index=[i])
                    
                
                # ongoing lactation
                elif a and f:
                    days_range = pd.date_range(start, lastday)
                    day_nums = pd.Series(range(1,len(days_range)+1), index=days_range)
                    wet_days1 = pd.DataFrame(day_nums, days_range)
                    
                    # get sum/max of series
                    wet1a = self.milk1.loc[start:stop, str(i)]
                    milking_liters1 = pd.Series(wet1a)
                    
                    wet_sum1 = pd.DataFrame([wet1a.sum()], columns=[j], index=[i])
                    wet_max1 = pd.DataFrame([wet1a.max()], columns=[j], index=[i])

                # no lactation
                elif e and f:
                    wet1a    = pd.DataFrame(columns=[j])
                    wet_sum1 = pd.DataFrame(columns=[j], index=[i])
                    wet_max1 = pd.DataFrame(columns=[j], index=[i])
                    
                                
                wet_days2 = pd.concat([wet_days2, wet_days1],axis=0)
                wet_sum2  = pd.concat([wet_sum2, wet_sum1], axis=1 )
                wet_max2  = pd.concat([wet_max2, wet_max1], axis=1 )
                
                milking_liters1 = milking_liters1.reset_index(drop=True)
                milking_liters2 = pd.concat([milking_liters2, milking_liters1], axis=1)

                
                wet_days1 = pd.DataFrame()
                wet_sum1 = wet_max1 = pd.DataFrame()
                milking_liters1 = pd.DataFrame()


                if not wet_days2.empty:
                # Remove duplicate indices before reindexing
                    wet_days2_clean = wet_days2[~wet_days2.index.duplicated(keep='last')]
                    wet_days2a = wet_days2_clean.reindex(idx)
                else:
                    wet_days2a = pd.DataFrame(index=idx)

                wet_days2b = wet_days2a .rename(columns = {0: i})

            
            
            # Fill NaN values with 0 before concatenation
            wet_days2b  = wet_days2b.astype(float)  .fillna(0)
            wet_sum2    = wet_sum2  .astype(float)  .fillna(0)
            wet_max2    = wet_max2  .astype(float)  .fillna(0)
                        
            if not wet_days2b.empty:
                wet_days3 = pd.concat( [wet_days3, wet_days2b],  axis=1) 
                wet_sum3 = pd.concat(  [wet_sum3, wet_sum2],    axis=0)
                wet_max3 = pd.concat(  [wet_max3, wet_max2],    axis=0)
            # print(f"wet_max3 after appending wet_max2 for WY_id {i}: {wet_max3}")
                
            wet_days2 = pd.DataFrame()
            wet_sum2 = pd.DataFrame()
            wet_max2 = pd.DataFrame()

        #initialize betore the if blcok
        wet_days_df1= pd.DataFrame()         
        wet_sum_df1 = pd.DataFrame()
        wet_max_df1 = pd.DataFrame()            
                
        if not wet_days3.empty:            
            wet_days_df1   = pd.DataFrame(wet_days3)
            wet_sum_df1    = pd.DataFrame(wet_sum3) 
            wet_max_df1    = pd.DataFrame(wet_max3)
            
        wsd1 = wet_sum_df1
        wsd2 = wsd1.reindex(self.MB.data['WY_ids'])
        self.wsd = wsd2
        
        wmd1 = wet_max_df1
        wmd2 = wmd1.reindex(self.MB.data['WY_ids'])
        self.wmd = wmd2
        
        self.wet_days_df = wet_days_df1
        self.milking_liters = milking_liters2
            
        return self.wet_days_df, self.wsd, self.wmd , self.milking_liters
    
    def create_dry_days(self):
        """
        Calculate dry days for each cow (WY_id) and lactation (lact_number).
        Dry days are the days between stop of one lactation and start of the next.
        Returns:
            dry_days_df: DataFrame (index=date, columns=WY_ids, values=day_num)
        """

        wy_ids = self.MB.data['WY_ids']
        lacts = self.MB.data['stop'].index
        idx = self.ext_rng
        dry_days_df = pd.DataFrame(index=idx, columns=wy_ids)

        for wy_id in wy_ids:
            prev_stop = None
            for i, lact in enumerate(lacts):
                start = self.MB.data['start'].at[lact, wy_id] if (lact in self.MB.data['start'].index and wy_id in self.MB.data['start'].columns) else None
                stop = self.MB.data['stop'].at[lact, wy_id] if (lact in self.MB.data['stop'].index and wy_id in self.MB.data['stop'].columns) else None
                # Dry period is after previous stop to this start
                if i == 0:
                    prev_stop = stop
                    continue
                prev_lact = lacts[i-1]
                prev_stop = self.MB.data['stop'].at[prev_lact, wy_id] if (prev_lact in self.MB.data['stop'].index and wy_id in self.MB.data['stop'].columns) else None
                this_start = start
                # Only if both prev_stop and this_start exist
                if pd.isna(prev_stop) or pd.isna(this_start):
                    continue
                # Dry days are from prev_stop+1 to this_start-1
                dry_start = pd.Timestamp(prev_stop) + pd.Timedelta(days=1)
                dry_end = pd.Timestamp(this_start) - pd.Timedelta(days=1)
                if dry_start > dry_end:
                    continue
                dry_range = pd.date_range(dry_start, dry_end)
                day_nums = pd.Series(range(1, len(dry_range)+1), index=dry_range)
                # Assign to DataFrame
                for d, n in day_nums.items():
                    if d in dry_days_df.index:
                        dry_days_df.at[d, wy_id] = n
        # Fill NaN with 0 and convert to int where possible
        dry_days_df = dry_days_df.fillna(0).infer_objects(copy=False)
        try:
            dry_days_df = dry_days_df.astype(int)
        except Exception:
            dry_days_df = dry_days_df.apply(pd.to_numeric, errors='ignore')
        
        self.dry_days_df = dry_days_df

        return self.dry_days_df

    def create_wet_dry_dict(self):

        wdd = self.wet_days_df
        milk1 = self.milk1
        wy_ids = self.MB.data['WY_ids']
        lact_numbers = list(self.MB.data['stop'].index)
        dry_days_df = self.dry_days_df
        result = {}

        for wy_id in wy_ids:
            wy_id_str = str(wy_id)
            result[wy_id_str] = {}

            for idx_lact, lact in enumerate(lact_numbers, 1):
                entries = []
                start = self.MB.data['start'].at[lact, wy_id] if (lact in self.MB.data['start'].index and wy_id in self.MB.data['start'].columns) else None
                stop = self.MB.data['stop'].at[lact, wy_id] if (lact in self.MB.data['stop'].index and wy_id in self.MB.data['stop'].columns) else None
                if pd.isna(start):
                    continue
                start_date = pd.Timestamp(start)
                if pd.isna(stop):
                    stop_date = wdd.index[-1]  #  last available date
                else:
                    stop_date = pd.Timestamp(stop)
                for date in wdd.index:
                    if not (start_date <= date <= stop_date):
                        continue
                    day_num = wdd.at[date, wy_id] if wy_id in wdd.columns else None
                    liters = milk1.at[date, wy_id_str] if (wy_id_str in milk1.columns and date in milk1.index) else None
                    if day_num is not None and not pd.isna(day_num) and day_num > 0:
                        liters_val = float(liters) if liters is not None and not pd.isna(liters) else None
                        entry = {
                            'date': str(date.date()) if hasattr(date, 'date') else str(date),
                            'liters': liters_val,
                            'revenue': liters_val * 22 if liters_val is not None else None,
                            'day_num': int(day_num) if not pd.isna(day_num) else None
                        }
                        entries.append(entry)
                if entries:
                    result[wy_id_str][f"L_{idx_lact}"] = entries
                    
            # --- Dry periods ---
            # Dry periods are between lactations, so up to len(lact_numbers)-1
            for idx_lact in range(1, len(lact_numbers)):
                entries = []
                # dry period after lact idx_lact (so between stop of lact idx_lact and start of lact idx_lact+1)
                prev_lact = lact_numbers[idx_lact-1]
                next_lact = lact_numbers[idx_lact]
                prev_stop = (
                    self.MB.data['stop'].at[prev_lact, wy_id]
                    if (
                        prev_lact in self.MB.data['stop'].index
                        and wy_id in self.MB.data['stop'].columns
                    )
                    else None
                )
                next_start = (
                    self.MB.data['start'].at[next_lact, wy_id]
                    if (
                        next_lact in self.MB.data['start'].index
                        and wy_id in self.MB.data['start'].columns
                    )
                    else None
                )
                if pd.isna(prev_stop) or pd.isna(next_start):
                    continue
                dry_start = pd.Timestamp(prev_stop) + pd.Timedelta(days=1)
                dry_end = pd.Timestamp(next_start) - pd.Timedelta(days=1)
                if dry_start > dry_end:
                    continue

                for date in dry_days_df.index:
                    if not (dry_start <= date <= dry_end):
                        continue
                    day_num = (
                        dry_days_df.at[date, wy_id]
                        if wy_id in dry_days_df.columns else None
                    )
                    if (
                        day_num is not None and not pd.isna(day_num) and day_num > 0
                    ):
                        entry = {
                            'date': str(date.date()) if hasattr(date, 'date') else str(date),
                            'liters': 0,
                            'revenue': 0,
                            'day_num': int(day_num) if not pd.isna(day_num) else None
                        }
                        entries.append(entry)
                if entries:
                    result[wy_id_str][f"D_{idx_lact}"] = entries

        self.wet_dry_dict = result
        return self.wet_dry_dict


    def flatten_dict_save_as_df(self):
        """
        Flatten the in-memory self.wet_dry_dict for all cows and periods into a DataFrame.
        Ensures correct dtypes and robust sorting.
        """
        if self.wet_dry_dict is None:
            print("wet_dry_dict is not initialized. Run create_wet_dry_dict first.")
            return
        rows = []
        for cow_id, periods in self.wet_dry_dict.items():
            for period, entries in periods.items():
                if isinstance(entries, list):
                    for entry in entries:
                        row = entry.copy()
                        row['period'] = period
                        row['WY_id'] = cow_id
                        rows.append(row)
                elif isinstance(entries, dict):
                    for date, entry in entries.items():
                        row = entry.copy()
                        row['period'] = period
                        row['WY_id'] = cow_id
                        row['date'] = date
                        rows.append(row)
        if not rows:
            print("No data found in wet_dry_dict.")
            return
        df = pd.DataFrame(rows)

        # Ensure correct dtypes
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        for col in ['liters', 'revenue', 'day_num', 'WY_id']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'period' in df.columns:
            df['period'] = df['period'].astype(str)

            # Reorder columns as: WY_id, date, day_num, period, liters, revenue
            sort_order = ['WY_id', 'date', 'day_num', 'period', 'liters', 'revenue']
            cols_in_df = [col for col in sort_order if col in df.columns]
            other_cols = [col for col in df.columns if col not in cols_in_df]
            df = df[cols_in_df + other_cols]

            # Sort by WY_id, date
            sort_cols = [c for c in ['WY_id', 'date'] if c in df.columns]
            if sort_cols:
                df = df.sort_values(sort_cols)

        self.wet_dry_df = df.reset_index(drop=True)
        return self.wet_dry_df


    def create_wetdry_weekly(self):
        """
        Group self.wet_dry_df by WY_id and week (using the 'date' column).
        Returns a DataFrame with weekly averages for each WY_id. Drops the 'period' column.
        """
        if self.wet_dry_df is None or self.wet_dry_df.empty:
            print("wet_dry_df is not initialized or empty. Run flatten_dict_save_as_df first.")
            return None
        df = self.wet_dry_df.copy()
        if 'date' not in df.columns or 'WY_id' not in df.columns:
            print("wet_dry_df must have 'date' and 'WY_id' columns.")
            return None
        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Only create 'period_week' column that restarts week numbering for each (WY_id, period)
        if 'period' in df.columns:
            df = df.sort_values(['WY_id', 'period', 'date'])
            week_starts = df['date'].dt.to_period('W').apply(lambda r: r.start_time)
            df['period_week'] = (
                df.groupby(['WY_id', 'period'])['date']
                .transform(lambda x: pd.factorize(x.dt.to_period('W').apply(lambda r: r.start_time))[0] + 1)
            )
        else:
            df = df.sort_values(['WY_id', 'date'])
            df['period_week'] = (
                df.groupby(['WY_id'])['date']
                .transform(lambda x: pd.factorize(x.dt.to_period('W').apply(lambda r: r.start_time))[0] + 1)
            )
 
        agg_dict = {}
        if 'liters' in df.columns:
            agg_dict['liters'] = 'mean'
        if 'revenue' in df.columns:
            agg_dict['revenue'] = 'mean'
        if 'period' in df.columns:
            agg_dict['period'] = 'first'
        if 'day_num' in df.columns:
            agg_dict['day_num'] = 'first'
        if 'date' in df.columns:
            agg_dict['date'] = 'first'  # Keep the earliest date for each week/cow/period

        group_cols = ['WY_id', 'period', 'period_week'] if 'period' in df.columns else ['WY_id', 'period_week']
        grouped = df.groupby(group_cols, as_index=False).agg(agg_dict)

        self.wet_dry_weekly = grouped
        return self.wet_dry_weekly
        

    def write_to_csv_json(self):
 
        with open('F:\\COWS\\data\\wet_dry\\wet_dry_dict.json', 'w', encoding='utf-8') as f:
            json.dump(self.wet_dry_dict, f, ensure_ascii=False, indent=2)


        self.wet_dry_df      .to_csv('F:\\COWS\\data\\wet_dry\\wet_dry_df.csv')       
        self.wsd             .to_csv('F:\\COWS\\data\\wet_dry\\wd_sum.csv')
        self.wmd             .to_csv('F:\\COWS\\data\\wet_dry\\wd_max.csv')
        self.wet_dry_weekly  .to_csv('F:\\COWS\\data\\wet_dry\\wet_dry_weekly.csv')
        



if __name__ == '__main__':
    obj=WetDry()
    obj.load_and_process()      