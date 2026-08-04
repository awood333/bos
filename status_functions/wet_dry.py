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
        self.period_df = None
        self.period_weekly = None
        self.wet_dry_days_weekly = None
        self.wet_dry_days = None
        self.wd_letters = None 
        self.wd_lact_num = None

        
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
        self.startdate  = self.MB.data['start']
        # self.startdate  = self.DR.startdate
      
        fullday         = self.MAB.fullday.copy()
        fullday.columns = fullday.columns.astype(str)
        self.milk1      = fullday.reindex(self.MB.data['ext_rng'])      
      
        self.bd = self.MB.data['bd']
        
        self.start_pivot = self.MB.data['start_pivot']
        self.stop_pivot  = self.MB.data['stop_pivot']

        self.wy_id_list = self.start_pivot.index
        self.lacts      = self.start_pivot.columns    

        #methods
        self.wet_dry_days, self.period_df   = self.create_wet_dry_table()
        self.period_weekly                  = self.create_period_weekly()
        self.wd_letters, self.wd_lact_num   = self.reform_period()
        self.wet_dry_days_weekly            = self.create_wet_dry_weekly()
        
          
    def create_wet_dry_table(self):
        '''  returns self.wet_dry_days, self.period_df  '''
        idx     = self.ext_rng
        wy_ids  = self.MB.data['wy_ids']
        lacts   = self.lacts #col headers from start_pivot
        lastday = self.MB.data['lastday']

        n_rows  = len(idx)
        day_num_array     = np.zeros((n_rows, len(wy_ids)))
        labels  = np.full((n_rows, len(wy_ids)), '', dtype=object)


# outer loop iterates over wy_ids
        for col, wy_id in enumerate(wy_ids):
            blocks = []
            label_blocks = []                                           
            first_start_date = None
            heifer_start = None
            prev_stop_date = None
            prev_lact = None
            earliest_date = None
            
            # --- gets the birth dates
            bd = self.bd[(self.bd['wy_id'] == wy_id)] #gets the one row in birth_death for this wy_id
            b_date1 = pd.to_datetime(bd['b_date']) # isolates the bdate
            b_date = pd.NaT if b_date1.empty else b_date1.iloc[0] # writes NaT for the empty slots (which don't exist)

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
# get a start and stop timestamp
                start = pd.to_datetime(self.start_pivot.at[wy_id, lact]
                        if (wy_id in self.start_pivot.index and lact in self.start_pivot.columns)
                        else np.nan)
                stop  = pd.to_datetime(self.stop_pivot.at[wy_id, lact]
                        if (wy_id in self.stop_pivot.index and lact in self.stop_pivot.columns)
                        else np.nan)

                if pd.isna(start):
                    continue
#first_start_date needs def here to differentiate cows from heifers
                if (first_start_date is None and (start > b_date)): 
                    first_start_date = pd.Timestamp(start)
                
# this sets up the initial lag of start and stop (essential for heifer definition)
                if (prev_stop_date is None)  and (stop < start):
                    prev_stop_date = stop


                if prev_stop_date is not None:
                    dry_start = pd.Timestamp(prev_stop_date) + pd.Timedelta(days=1)
                    dry_end   = pd.Timestamp(start) - pd.Timedelta(days=1)
                    if dry_start <= dry_end:
                        n_dry = (dry_end - dry_start).days + 1
                        blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))
                        label_blocks.append(np.full((n_dry, 1), f'D{prev_lact}', dtype=object))


#this sets up 'milking' currently -- if 'stop' is blank
                wet_stop = lastday if pd.isna(stop) else pd.Timestamp(stop)
                if wet_stop < pd.Timestamp(start):
                    print(f"wy_id {wy_id}, lact {lact}: stop ({wet_stop.date()}) before start ({pd.Timestamp(start).date()}), skipped")
                    prev_stop_date = None if pd.isna(stop) else pd.Timestamp(stop)
                    prev_lact = lact
                    continue
                n_wet = (wet_stop - pd.Timestamp(start)).days + 1
                blocks.append(np.arange(1, n_wet + 1).reshape(-1, 1))
                label_blocks.append(np.full((n_wet, 1), f'W{lact}', dtype=object))

                prev_stop_date = None if pd.isna(stop) else pd.Timestamp(stop)
                prev_lact = lact

# --- trailing period (gone or dry) ---
            if prev_stop_date is not None and prev_stop_date < lastday:
                if pd.notna(death_date_val):
                    # Cow died – dry period from last stop to death_date, then zero days after death
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
                        label_blocks.append(np.full((n_gone, 1), '', dtype=object))  #no more 'gone'
                else:
                    # Alive – dry block to lastday
                    block_start = prev_stop_date + pd.Timedelta(days=1)
                    block_end   = lastday
                    if block_start <= block_end:
                        n_dry = (block_end - block_start).days + 1
                        blocks.append(np.arange(1, n_dry + 1).reshape(-1, 1))
                        label_blocks.append(np.full((n_dry, 1), f'D{prev_lact}', dtype=object))
            
                    
                    
# --- heifer period from birth to first_start_date-1 (cows with lactations) ---
            if not pd.isna(b_date) and first_start_date is not None and b_date < first_start_date:
                heifer_end = first_start_date - pd.Timedelta(days=1)
                heifer_start = max(b_date, idx.min())
                heifer_end   = min(heifer_end, idx.max())
                if heifer_start <= heifer_end:
                    n_heifer = (heifer_end - heifer_start).days + 1
                    blocks.insert(0, np.arange(1, n_heifer + 1).reshape(-1, 1))
                    label_blocks.insert(0, np.full((n_heifer, 1), 'H0', dtype=object))
                    earliest_date = heifer_start
                else:
                    earliest_date = first_start_date


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
            labels[row_offset:row_offset + rows_to_fill, col] = stacked_labels[:rows_to_fill, 0]

        wet_dry_table1      = pd.DataFrame(day_num_array, index=idx, columns=wy_ids)
        self.wet_dry_days  = wet_dry_table1.loc[self.startdate: , :]
        period_df1          = pd.DataFrame(labels, index=idx, columns=wy_ids).copy()
        self.period_df      = period_df1.loc[self.startdate :, :].copy()
        return self.wet_dry_days, self.period_df
    


    def create_period_weekly(self, freq='W'):
        
        ''' converts the daily df self.period_df to weekly'''
        self.period_weekly = self.period_df.resample(freq).last()
            
        self.wet_period_weekly = self.period_weekly[
            self.period_weekly.index  >= pd.to_datetime(self.startdate)]\
            .reset_index().rename(columns={'index': 'date'}).set_index('date')
        return self.period_weekly
    

        
    def reform_period(self):
        ''' the self.period df comes from wet_dry and has eg W1 for each date for each cow
            This 'reform' splits the period into the W and the number: two df's 'letters' and 'wd_lact_num '''
        df = self.period_weekly
        regex_pattern = r'([A-Za-z]+)(\d+)'
        #([A-Za-z]+) captures one or more letters (the W, D, whatever prefix)
        #(\d+) captures one or more digits (the number)
        self.wd_letters  = df.apply(lambda col: col.str.extract(regex_pattern)[0])
        self.wd_lact_num = df.apply(lambda col: col.str.extract(regex_pattern)[1]).astype(float)
        return self.wd_letters, self.wd_lact_num
            
  
    def create_wet_dry_weekly(self, freq='W'):
        '''Weekly aggregation of wet_dry_days (numeric) using last value.'''
        weekly_last = self.wet_dry_days.resample(freq).last()
        
        self.wet_dry_days_weekly = weekly_last.apply(
            lambda col: col.map(lambda x: 0 if x == 0 else (x - 1) // 7 + 1))
        
        self.wet_dry_days_weekly  = self.wet_dry_days_weekly[
            self.wet_dry_days_weekly.index >= pd.to_datetime(self.startdate)]\
            .reset_index().rename(columns={'index': 'date'}).set_index('date')
            
        return self.wet_dry_days_weekly




if __name__ == '__main__':
    obj=WetDry()
    obj.load()      