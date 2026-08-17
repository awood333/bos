'''
status_functions.wet_dry
'''
import inspect
import pandas as pd
import numpy as np
from container import get_dependency

today = pd.Timestamp.today()

class WetDry:
    ''' returns the 'period' W1, D1 etc, and the days '''
        
    def __init__(self):
        print(f"WetDry instantiated by: {inspect.stack()[1].filename}")

        # load
        self.MB = None
        
        #process
        self.data = None
        self.death_date = None
        self.ext_rng = None
        self.datex = None        
        self.startdate = None
        
        self.milk1 = None
        self.bd = None
        self.start_pivot = None
        self.stop_pivot = None
        self.wy_id_list = None
        self.lacts = None
        
        #methods
        self.period_daily = None
        self.wd_letters_daily = None 
        self.wd_lact_num_daily = None
        self.period_weekly      = None
        self.wet_period_weekly  = None
        self.wet_dry_days_weekly = None
        self.wet_dry_days_daily = None
        self.wd_letters_weekly = None 
        self.wd_lact_num_weekly = None

        
    def load(self):
        self.MB = get_dependency('milk_basics')
    
        # Get fullday DataFrame from MilkAggregatesBasic and reindex to extended date range
        # Columns are integer indices from numpy; convert to strings to match str(wy_id) lookups
        self.MAB = get_dependency('milk_aggregates_basic')
        self.DR = get_dependency('date_range')
        self.process()
        
    def process(self):
        self.data       = self.MB.data
        self.death_date = self.MB.bd[['wy_id','death_date']]
        self.ext_rng    = self.MB.data['ext_rng']
        self.datex      = self.MB.data['datex']
        # self.startdate  = self.MB.data['start']
        self.startdate  = self.DR.startdate
      
        fullday         = self.MAB.fullday.copy()
        fullday.columns = fullday.columns.astype(str)
        self.milk1      = fullday.reindex(self.MB.data['ext_rng'])      
      
        self.bd = self.MB.data['bd']
        
        self.start_pivot = self.MB.data['start_pivot']
        self.stop_pivot  = self.MB.data['stop_pivot']

        self.wy_id_list = self.start_pivot.index
        self.lacts      = self.start_pivot.columns    

        #methods
        (self.wet_dry_days_daily, 
         self.period_daily)     = self.create_wet_dry_daily()
        
        (self.wd_letters_daily, 
        self.wd_lact_num_daily) = self.reform_period_daily()
        
        
        self.period_weekly      = self.create_period_weekly()
        
        (self.wd_letters_weekly, 
         self.wd_lact_num_weekly)   = self.reform_period_weekly()
        
        self.wet_dry_days_weekly    = self.create_wet_dry_days_weekly()
        
          
    def create_wet_dry_daily(self):
        '''  returns self.wet_dry_days_daily, self.period_daily  '''
        
        wy_ids  = self.MB.data['wy_ids']
        lacts   = self.lacts #col headers from start_pivot
        lastday = self.MB.data['lastday']
        
        idx     = self.ext_rng #starting 2016-09-01
        n_rows  = len(idx)
        day_num_array   = np.zeros((n_rows, len(wy_ids)))
        period_array    = np.full ((n_rows, len(wy_ids)), '', dtype=object)


# outer loop iterates over wy_ids.  
# enumerate(wy_ids) returns a sequence of tuples: (0, wy_id_0)
        for col, wy_id in enumerate(wy_ids):
            blocks = []
            label_blocks = []                                           
            first_start_date    = None
            heifer_start        = None
            prev_stop_date      = None
            prev_lact           = None
            earliest_date       = None
            has_lactation_blocks = False 
            
            # --- gets the birth dates
            bd = self.bd[(self.bd['wy_id'] == wy_id)] #gets the one row in birth_death for this wy_id
            b_date1 = bd['b_date'] # isolates the bdate
            b_date = pd.NaT if b_date1.empty else b_date1.iloc[0] # Timestamp... writes NaT for the empty slots (which don't exist)

            # arrival_date = 'arrived' if present, else fall back to b_date
            arrival_date = b_date
            if not bd.empty and 'arrived' in bd.columns:
                av = bd['arrived'].iloc[0]
                if pd.notna(av):
                    arrival_date = pd.Timestamp(av)


            # --- gets death dates ---            
            death_date_val = pd.NaT
            if not bd.empty and 'death_date' in bd.columns:
                dd_val = bd['death_date'].iloc[0]
                try:
                    death_date_val = pd.Timestamp(dd_val)
                except (ValueError, TypeError):
                    death_date_val = pd.NaT

#inner loop iterates over lactation numbers
            for lact in lacts:
                # get a start_day and stop_day as timestamp
                start_day = pd.to_datetime(self.start_pivot.at[wy_id, lact]
                        if (wy_id in self.start_pivot.index and lact in self.start_pivot.columns)
                        else np.nan)
                stop_day  = pd.to_datetime(self.stop_pivot.at[wy_id, lact]
                        if (wy_id in self.stop_pivot.index and lact in self.stop_pivot.columns)
                        else np.nan)

                if pd.isna(start_day):
                    continue
                #first_start_date needs def here to differentiate cows from heifers
                if (first_start_date is None and (start_day > b_date)): 
                    first_start_date = pd.Timestamp(start_day)
                
# this sets up the initial lag of start_day and stop_day (essential for heifer definition)
                if (prev_stop_date is None)  and (stop_day < start_day):
                    prev_stop_date = stop_day
                    #here, prev_stop_date is the (future) stop_day
                    
                if prev_stop_date is not None:
                    dry_start = pd.Timestamp(prev_stop_date) + pd.Timedelta(days=1)
                    dry_end   = pd.Timestamp(start_day) - pd.Timedelta(days=1)
                    if dry_start <= dry_end:
                        n_dry = (dry_end - dry_start).days + 1
                        blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))
                        label_blocks.append(np.full((n_dry, 1), f'D{prev_lact}', dtype=object))


#this sets up 'milking' currently -- if 'stop_day' is blank
                wet_stop = lastday if pd.isna(stop_day) else pd.Timestamp(stop_day)
                if wet_stop < pd.Timestamp(start_day):
                    # print(f"wy_id {wy_id}, lact {lact}: stop_day ({wet_stop.date()}) before start_day ({pd.Timestamp(start_day).date()}), skipped")
                    prev_stop_date = None if pd.isna(stop_day) else pd.Timestamp(stop_day)
                    prev_lact = lact
                    continue
                n_wet = (wet_stop - pd.Timestamp(start_day)).days + 1
                blocks.append(np.arange(1, n_wet + 1).reshape(-1, 1))
                label_blocks.append(np.full((n_wet, 1), f'W{lact}', dtype=object))
                has_lactation_blocks = True

                prev_stop_date = None if pd.isna(stop_day) else pd.Timestamp(stop_day)
                prev_lact = lact

# --- trailing period (gone or dry) ---
            if prev_stop_date is not None and prev_stop_date < lastday:
                if pd.notna(death_date_val):
                    # Cow died – dry period from last stop_day to death_date, then zero days after death
                    if prev_stop_date < death_date_val:
                        dry_start = prev_stop_date + pd.Timedelta(days=1)
                        dry_end   = death_date_val
                        if dry_start <= dry_end:
                            n_dry = (dry_end - dry_start).days + 1
                            blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))
                            label_blocks.append(np.full((n_dry, 1), f'D{prev_lact}', dtype=object))
                    # Gone period (zero days)
                    if death_date_val < lastday:
                        gone_start = death_date_val + pd.Timedelta(days=1)
                        gone_end   = lastday
                        n_gone = (gone_end - gone_start).days + 1
                        blocks.append(np.zeros((n_gone, 1)))          # keep as a placeholder
                        label_blocks.append(np.full((n_gone, 1), 'gone', dtype=object))
                else:
                    # Alive – dry block to lastday
                    block_start = prev_stop_date + pd.Timedelta(days=1)
                    block_end   = lastday
                    if block_start <= block_end:
                        n_dry = (block_end - block_start).days + 1
                        blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))
                        label_blocks.append(np.full((n_dry, 1), f'D{prev_lact}', dtype=object))
            
                    
                    
# --- heifer period from birth to first_start_date-1 (cows with lactations) ---
            # --- heifer period from arrival (or birth) to first start date-1 ---
            heifer_birth = arrival_date if pd.notna(arrival_date) else b_date

            if not pd.isna(heifer_birth):
                if first_start_date is not None and heifer_birth < first_start_date:
                    heifer_end = first_start_date - pd.Timedelta(days=1)
                else:
                    heifer_end = lastday

                # Cap heifer period at death date so dead heifers don't show H0 after death
                if pd.notna(death_date_val):
                    heifer_end = min(heifer_end, death_date_val)

                heifer_start = max(heifer_birth, idx.min())
                heifer_end   = min(heifer_end, idx.max())

                if heifer_start <= heifer_end:
                    n_heifer = (heifer_end - heifer_start).days + 1
                    blocks.insert(0, np.arange(1, n_heifer + 1).reshape(-1, 1))
                    label_blocks.insert(0, np.full((n_heifer, 1), 'H0', dtype=object))
                    earliest_date = heifer_start
                elif first_start_date is not None:
                    earliest_date = first_start_date

            # For dead cows with no lactations, add a 'gone' block after death
            if (pd.notna(death_date_val) and death_date_val < lastday
                    and not has_lactation_blocks):
                gone_start = max(death_date_val + pd.Timedelta(days=1), idx.min())
                gone_end   = min(lastday, idx.max())
                if gone_start <= gone_end:
                    n_gone = (gone_end - gone_start).days + 1
                    blocks.append(np.zeros((n_gone, 1)))
                    label_blocks.append(np.full((n_gone, 1), 'gone', dtype=object))
                    if earliest_date is None:
                        earliest_date = gone_start                        

            if not blocks or earliest_date is None:
                continue

            stacked = np.vstack(blocks)
            stacked_labels = np.vstack(label_blocks)

            try:
                row_offset = idx.get_loc(earliest_date)
            except KeyError:
                continue

            n = stacked.shape[0]
            rows_to_fill = min(n, n_rows - row_offset)
            day_num_array[row_offset:row_offset + rows_to_fill, col] = stacked[:rows_to_fill, 0]
            period_array [row_offset:row_offset + rows_to_fill, col] = stacked_labels[:rows_to_fill, 0]

        wet_dry_table1      = pd.DataFrame(day_num_array, index=idx, columns=wy_ids)
        #startdate is 2016-09-01
        self.wet_dry_days_daily  = wet_dry_table1.loc[self.startdate: , :]
        period_df1          = pd.DataFrame(period_array, index=idx, columns=wy_ids).copy()
        self.period_daily   = period_df1.loc[self.startdate :, :].copy()
        return self.wet_dry_days_daily, self.period_daily
    
        
    def reform_period_daily(self):
        ''' the self.period df comes from wet_dry and has eg W1 for each date for each cow
            This 'reform' splits the period into the W and the number: two df's 'letters' and 'wd_lact_num_weekly '''
        df = self.period_daily
        regex_pattern = r'([A-Za-z]+)(\d+)'
        #([A-Za-z]+) captures one or more letters (the W, D, whatever prefix)
        #(\d+) captures one or more digits (the number)
        self.wd_letters_daily  = df.apply(lambda col: col.str.extract(regex_pattern)[0])
        self.wd_lact_num_daily = df.apply(lambda col: col.str.extract(regex_pattern)[1]).astype(float)
        return self.wd_letters_daily, self.wd_lact_num_daily




#-----WEEKLY CONVERSION-----**********************************************

    def create_period_weekly(self, freq='W'):
        
        ''' converts the daily df self.period_daily to weekly'''
        
        self.period_weekly = self.period_daily.resample(freq).last()
            
        self.wet_period_weekly = self.period_weekly[
            self.period_weekly.index  >= self.startdate] \
                .reset_index().rename(columns={'index': 'date'}) \
                    .set_index('date') #startdate is 2016-09-01
        return self.period_weekly
    

        
    def reform_period_weekly(self):
        ''' the self.period df comes from wet_dry and has eg W1 for each date for each cow
            This 'reform' splits the period into the W and the number: two df's 'letters' and 'wd_lact_num_weekly '''
        df = self.period_weekly
        regex_pattern = r'([A-Za-z]+)(\d+)'
        #([A-Za-z]+) captures one or more letters (the W, D, whatever prefix)
        #(\d+) captures one or more digits (the number)
        self.wd_letters_weekly  = df.apply(lambda col: col.str.extract(regex_pattern)[0])
        self.wd_lact_num_weekly = df.apply(lambda col: col.str.extract(regex_pattern)[1]).astype(float)
        return self.wd_letters_weekly, self.wd_lact_num_weekly
            
  
    def create_wet_dry_days_weekly(self, freq='W'):
        '''Weekly aggregation of wet_dry_days (numeric) using last value.'''
        weekly_last = self.wet_dry_days_daily.resample(freq).last()
        
        self.wet_dry_days_weekly = weekly_last.apply(
            lambda col: col.map(lambda x: 0 if x == 0 else (x - 1) // 7 + 1))
        
        self.wet_dry_days_weekly  = self.wet_dry_days_weekly[
            self.wet_dry_days_weekly.index >= self.startdate] \
                .reset_index().rename(columns={'index': 'date'}) \
                    .set_index('date')
                
        return self.wet_dry_days_weekly




if __name__ == '__main__':
    obj=WetDry()
    obj.load()      