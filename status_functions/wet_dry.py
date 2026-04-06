'''
status_functions.wet_dry
'''
import inspect
import pandas as pd
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
        self.WY_ids = None
        self.startdate = None
        self.alive_ids = None

        self.wsd = None
        self.wmd = None
        self.milking_liters = None
        self.wet_days_df = None
        self.dry_days_df = None
        self.wdd_monthly = None
        self.wet_dry_df = None
        self.wet_dry_weekly = None

    def load_and_process(self):
        self.MB = get_dependency('milk_basics')
        self.data = self.MB.data
        self.death_date = self.MB.bd[['WY_id','death_date']]
        self.ext_rng = self.MB.data['ext_rng']
        self.milk1 = self.MB.data['milk']
        self.datex = self.MB.data['datex']
        self.WY_ids = self.MB.data['start'].columns
        DR = get_dependency('date_range')
        self.startdate = DR.startdate
        SD     = get_dependency('status_data')
        self.alive_ids = SD.alive_ids.loc[self.startdate,:]

        [self.wet_days_df, self.wsd, 
         self.wmd, self.milking_liters] = self.create_wet_days()

        self.dry_days_df = self.create_dry_days()

        self.wet_dry_df   = self.create_wet_dry_df()
        self.wet_dry_weekly = self.create_wetdry_weekly()
        self.write_to_csv()


    def create_wet_days(self):

        wet_days1 = wet_days2 = wet_days3 = pd.DataFrame()
        wet_sum_df1 = wet_sum2 = wet_sum3 = pd.DataFrame()
        wet_max_df1 = wet_max2 = wet_max3 = pd.DataFrame()
        milking_liters1 = milking_liters2 = pd.DataFrame()        
        idx     = self.ext_rng
        # WY_ids  = self.MB.data['WY_ids']   # this runs thru ALL the cows

        WY_ids_alive = self.alive_ids.astype(int)  #this funs thru the cows alive on 'startdate' 
                #some will not be milking 'now' but the data is useful in judging whether to keep the cow or not.

        i=0
        
        lacts   = self.MB.data['stop'].index      # lact# (float)

        for i in WY_ids_alive:
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
        # print(self.wet_days_df.loc['2026-01-09':'2026-01-11',250])
            
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

    def create_wet_dry_df(self):
        """
        Build wet_dry_df directly from wet_days_df, dry_days_df, and milk1.
        No intermediate dict — pure vectorized pandas operations.
        """
        wdd = self.wet_days_df
        milk1 = self.milk1
        wy_ids = self.MB.data['WY_ids']
        lact_numbers = list(self.MB.data['stop'].index)
        dry_days_df = self.dry_days_df

        # --- 1. Stack wet_days_df → (date, WY_id, day_num) where day_num > 0 ---
        wet = wdd.stack().reset_index()
        wet.columns = ['date', 'WY_id', 'day_num']
        wet = wet[wet['day_num'] > 0].copy()

        # --- 2. Stack milk1 → (date, WY_id, liters) and join ---
        milk_s = milk1.stack().reset_index()
        milk_s.columns = ['date', 'WY_id_str', 'liters']
        milk_s['WY_id'] = pd.to_numeric(milk_s['WY_id_str'], errors='coerce')
        milk_s = milk_s.drop(columns='WY_id_str')
        wet = wet.merge(milk_s, on=['date', 'WY_id'], how='left')
        wet['revenue'] = wet['liters'] * 22

        # --- 3. Build period lookup for wet (L_1, L_2, …) ---
        wet_periods = []
        for idx_lact, lact in enumerate(lact_numbers, 1):
            for wy_id in wy_ids:
                start = self.MB.data['start'].at[lact, wy_id] if (
                    lact in self.MB.data['start'].index and wy_id in self.MB.data['start'].columns) else None
                if pd.isna(start):
                    continue
                stop = self.MB.data['stop'].at[lact, wy_id] if (
                    lact in self.MB.data['stop'].index and wy_id in self.MB.data['stop'].columns) else None
                start_date = pd.Timestamp(start)
                stop_date = wdd.index[-1] if pd.isna(stop) else pd.Timestamp(stop)
                wet_periods.append({'WY_id': wy_id, 'period': f'L_{idx_lact}',
                                    'p_start': start_date, 'p_stop': stop_date})
        wet_plookup = pd.DataFrame(wet_periods)

        # Assign period to each wet row via range-join
        wet = wet.merge(wet_plookup, on='WY_id', how='inner')
        wet = wet[(wet['date'] >= wet['p_start']) & (wet['date'] <= wet['p_stop'])].copy()
        wet = wet.drop(columns=['p_start', 'p_stop'])

        # --- 4. Stack dry_days_df → (date, WY_id, day_num) where day_num > 0 ---
        dry = dry_days_df.stack().reset_index()
        dry.columns = ['date', 'WY_id', 'day_num']
        dry = dry[dry['day_num'] > 0].copy()
        dry['liters'] = 0
        dry['revenue'] = 0

        # --- 5. Build period lookup for dry (D_1, D_2, …) ---
        dry_periods = []
        for idx_lact in range(1, len(lact_numbers)):
            prev_lact = lact_numbers[idx_lact - 1]
            next_lact = lact_numbers[idx_lact]
            for wy_id in wy_ids:
                prev_stop = self.MB.data['stop'].at[prev_lact, wy_id] if (
                    prev_lact in self.MB.data['stop'].index and wy_id in self.MB.data['stop'].columns) else None
                next_start = self.MB.data['start'].at[next_lact, wy_id] if (
                    next_lact in self.MB.data['start'].index and wy_id in self.MB.data['start'].columns) else None
                if pd.isna(prev_stop) or pd.isna(next_start):
                    continue
                dry_start = pd.Timestamp(prev_stop) + pd.Timedelta(days=1)
                dry_end   = pd.Timestamp(next_start) - pd.Timedelta(days=1)
                if dry_start > dry_end:
                    continue
                dry_periods.append({'WY_id': wy_id, 'period': f'D_{idx_lact}',
                                    'p_start': dry_start, 'p_stop': dry_end})
        dry_plookup = pd.DataFrame(dry_periods)

        if not dry_plookup.empty and not dry.empty:
            dry = dry.merge(dry_plookup, on='WY_id', how='inner')
            dry = dry[(dry['date'] >= dry['p_start']) & (dry['date'] <= dry['p_stop'])].copy()
            dry = dry.drop(columns=['p_start', 'p_stop'])
        else:
            dry = dry.head(0)  # empty with correct columns
            dry['period'] = None

        # --- 6. Concat wet + dry → final DataFrame ---
        df = pd.concat([wet, dry], ignore_index=True)
        df['WY_id'] = pd.to_numeric(df['WY_id'], errors='coerce')
        df['day_num'] = pd.to_numeric(df['day_num'], errors='coerce')

        col_order = ['WY_id', 'date', 'day_num', 'period', 'liters', 'revenue']
        cols_in_df = [c for c in col_order if c in df.columns]
        other_cols = [c for c in df.columns if c not in cols_in_df]
        df = df[cols_in_df + other_cols]
        df = df.sort_values(['WY_id', 'date']).reset_index(drop=True)

        self.wet_dry_df = df
        return self.wet_dry_df


    def create_wetdry_weekly(self):
        """
        Group self.wet_dry_df by WY_id and week (using the 'date' column).
        Returns a DataFrame with weekly averages for each WY_id. Drops the 'period' column.
        """
        if self.wet_dry_df is None or self.wet_dry_df.empty:
            print("wet_dry_df is not initialized or empty.")
            return None
        df = self.wet_dry_df.copy()
        if 'date' not in df.columns or 'WY_id' not in df.columns:
            print("wet_dry_df must have 'date' and 'WY_id' columns.")
            return None
        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Only create 'period_week' column that restarts week numbering for each (WY_id, period)
        df = df.sort_values(['WY_id', 'period', 'date'])
        df['period_week'] = (
            df.groupby(['WY_id', 'period'])['date']
            .transform(lambda x: pd.factorize(x.dt.to_period('W').apply(lambda r: r.start_time))[0] + 1)
        )

        grouped = df.groupby(['WY_id', 'period', 'period_week'], as_index=False).agg(
            liters=('liters',  'mean'),
            revenue=('revenue', 'mean'),
            period=('period',  'first'),
            day_num=('day_num', 'first'),
            date=('date',    'first'),
        )
        self.wet_dry_weekly = grouped
        return self.wet_dry_weekly
        

    def write_to_csv(self):

        self.wet_dry_df      .to_csv('F:\\COWS\\data\\wet_dry\\wet_dry_df.csv')       
        self.wsd             .to_csv('F:\\COWS\\data\\wet_dry\\wd_sum.csv')
        self.wmd             .to_csv('F:\\COWS\\data\\wet_dry\\wd_max.csv')
        self.wet_dry_weekly  .to_csv('F:\\COWS\\data\\wet_dry\\wet_dry_weekly.csv')
        



if __name__ == '__main__':
    obj=WetDry()
    obj.load_and_process()      