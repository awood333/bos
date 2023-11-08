'''
status.py
'''
import pandas as pd
import numpy as np
from startdate_funct import CreateStartdate

class StatusData:
    
    def __init__(self):
        self.f1        = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0,  header=0, 
                            parse_dates=['datex'])
        
        self.bd        = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',      index_col=0, header=0,
                            parse_dates=['birth_date','death_date','arrived', 'adj_bdate'])
        
        self.lb_last   = pd.read_csv('F:\\COWS\\data\\insem_data\\lb_last.csv',         index_col=0, header=0,
                            parse_dates=['last calf bdate'])
        
        self.maxdate        = self.f1.index.max() 
        self.stopdate       = self.maxdate 
        self.bdmax          = len(self.bd)
        self.wy_series      = pd.Series(list(range(1, self.bdmax + 1)), name='WY_id', index=range(1, self.bdmax + 1))
        
        
        # functions

        self.startdate,  self.date_format, self.date_range, self.date_range_cols,           = self.import_startdate()
        
        self.f, self.datex, self.rshape, self.stackresult                                   = self.create_partition_milk_df()
                
        self.y                                      = self.create_stack_df(None)
        self.alive_mask, self.alive_count_df, self.gone_mask, self.gone_count_df,self.nby_count_df,   self.ungone, self.allcows = self.create_alive_mask()
        
        self.alive_ids                              = self.create_alive_ids()
        self.gone_ids                               = self.create_gone_ids()           
        
        self.milkers_mask,  self.milkers_count      = self.create_milkers_mask()
        self.milkers_ids                            = self.create_milkers_ids()
        
        self.days_milking_df, self.days_mean        = self.create_days()
        self.group_a ,  self.group_a_count          = self.create_group_a()
        self.group_b ,  self.group_b_count          = self.create_group_b()
         
        self.group_a_ids                            = self.create_group_a_ids()
        self.group_b_ids                            = self.create_group_b_ids()
        
        self.dry_ids, self.dry_count, self.dry_mask = self.create_dry_ids()
        
        self.allcows                                = self.allcows_summary()
        self.status_col                             = self.create_status_col()
        self.create_write_to_csv()      

        
    def import_startdate(self):
        create_startdate_instance = CreateStartdate()
        startdate =  create_startdate_instance.startdate
        date_format = create_startdate_instance.date_format
        
        date_range      = pd.date_range(startdate, self.stopdate, freq='D')     #stopdate comes from maxdate above
        date_range_cols = date_range.strftime(date_format).to_list()
        
        return startdate, date_format, date_range, date_range_cols
    
    
    def create_partition_milk_df(self):
        f = self.f1.loc[self.date_range, :]     # partitions the milk df
        datex          = f.index.to_list()
        rshape         = f.shape[1]
        stackresult    = np.full((0, rshape), np.nan)  #creates the empty array for 'results'
        
        return f, datex, rshape, stackresult
        
    
    
    def create_stack_df(self, x=None):
        y1=[]
        if x is None:
            return pd.DataFrame()
        
        for index, row in x.iterrows():
            
            x_series = pd.Series(row.dropna().values.tolist())
            padsize = self.rshape - len(x_series)  #rshape is the cols of f, len(x) is the rows of one slice
            
            x2 = pd.Series(x_series.fillna(0).values)
            y1.append(np.pad(x2, (0, padsize)))
        y = np.vstack(y1)

        return y



    def create_alive_mask(self):   

        alive_mask = pd.DataFrame(index=self.date_range, columns=range(1, len(self.bd) + 1), dtype=bool)
        gone_mask  = pd.DataFrame(index=self.date_range, columns=range(1, len(self.bd) + 1), dtype=bool)
        nby_mask   = pd.DataFrame(index=self.date_range, columns=range(1, len(self.bd) + 1), dtype=bool)

        for i in alive_mask.columns:
                
            var1 = self.bd['adj_bdate'][i]
            var2 = self.bd['death_date'][i] 
    
            condition1 = (
                (var1 <= self.date_range) &
                ((var2 > self.date_range) | (pd.isnull(var2) == True))
            )
            alive_mask[i] = condition1    #cow is alive and not gone  yet
                
                
            condition2 = (
                (var1 <= self.date_range) &  
                ((var2 <= self.date_range) & (pd.isnull(var2) == False))
            )    
            gone_mask[i] = condition2   #cow was alive, but is now gone
            
                        
            condition3 = (
                (var1 > self.date_range) &
                ((var2 > self.date_range) | (pd.isnull(var2) == True))
            )    
            nby_mask[i] = condition3    #cow isn't born yet on this date
            

        alive_count_df = alive_mask.copy().sum(axis=1).to_frame(name='alive')    
        gone_count_df  = gone_mask.sum(axis=1) .to_frame(name='gone') 
        nby_count_df   = nby_mask.sum(axis=1)  .to_frame(name='nby')    
        
        ungone = alive_count_df.join(nby_count_df)
        allcows= ungone.join(gone_count_df)
        allcows['sum']=allcows.sum(axis=1)
        return alive_mask, alive_count_df,   gone_mask, gone_count_df,  nby_count_df,   ungone, allcows

 

    def create_alive_ids(self):     
        alive_ids1 = self.alive_mask.copy(deep=True)                
        for col in self.alive_mask.columns: 
            alive_ids1[col] = np.where(self.alive_mask[col], col, np.nan)
            
        x           = alive_ids1
        alive_ids2  = self.create_stack_df(x)  # this passes x as an argument
        alive_ids   = pd.DataFrame(alive_ids2, index=[self.datex])
        
        return alive_ids
    
    
    
    def create_gone_ids(self):     
        gone_ids1 = self.gone_mask.copy(deep=True)                
        for col in self.gone_mask.columns: 
            gone_ids1[col] = np.where(self.gone_mask[col], col, np.nan)
            
        x           = gone_ids1    
        gone_ids2   = self.create_stack_df(x)
        gone_ids    = pd.DataFrame(gone_ids2, index=[self.datex])
        return gone_ids

    def create_milkers_mask(self):
        milkers_mask        = self.f > 0   
        milkers_count       = milkers_mask.sum(axis=1).to_frame(name='milkers')
        return milkers_mask, milkers_count



    def create_milkers_ids(self):      
        milkers_ids1 = self.milkers_mask.copy(deep=True)
        for col in self.milkers_mask.columns:
            milkers_ids1[col] = np.where(self.milkers_mask[col], col, np.nan)      #returns the col header where True, or np.nan for the False
 
        x = milkers_ids1    
        
        milkers_ids2    = self.create_stack_df(x)
        milkers_ids     = pd.DataFrame(milkers_ids2, index=[self.datex])
        return milkers_ids
    


    def create_days(self):  
        truecols = []
        days1, days     = [], [] 
        
        for index, row in self.milkers_mask.iterrows():
            true_columns   = [col for col, value in row.items() if value == True]
            truecols.append(true_columns)   #this accumulates the rows - o/wise only the last row is kept

        for index, row in self.milkers_mask.iterrows():  #creates a list of dict key:val pairs 
            days_result     = {}
            for cols in truecols:
                for col in cols:
                    if row[col] == True:        
                        lastcalf_bdate  = self.lb_last.loc[int(col), 'last calf bdate'  ]
                        date_diff       = (index - lastcalf_bdate).days
                        days_result[col]    = date_diff
            days1.append(days_result)
        days_milking_df = pd.DataFrame(days1, index=self.date_range_cols)
        days_milking_df.index.name = 'datex'
        
        days_mean       = days_milking_df.mean(axis=1)
        days_mean.index.name = 'datex'
        
        return days_milking_df, days_mean  



    def create_group_a(self):

        days_df        = self.days_milking_df
        group_a        = days_df < 140          #create 2D bool mask

        group_a_count        = group_a.sum(axis=1).to_frame(name='group_a')              #count of True in each row (date)
        group_a_count.index = self.date_range    #date index and scalar for count on that day
        group_a_count.index.name = 'datex'
        return group_a, group_a_count



        
    def create_group_b(self):

        days_df        = self.days_milking_df.copy()
        group_b        = days_df >= 140
                
        group_b_count               = group_b.sum(axis=1).to_frame(name='group_b')              #count of True in each row (date)
        group_b_count.index         = self.date_range
        group_b_count.index.name    = 'datex'
        
        return group_b, group_b_count



    def create_group_a_ids(self):     
        result              = self.group_a.copy()
        for col in self.group_a.columns:
            result[col]     = np.where(self.group_a[col], col, np.nan)      #returns the col header where True, or np.nan for the False
        result.index        = self.date_range
        group_a_ids      = result #pd.DataFrame(result, columns=group_a.columns, index=self.date_range_cols)
        return group_a_ids




    def create_group_b_ids(self):     

        result              = self.group_b
        for col in self.group_b.columns:
            result[col]     = np.where(self.group_b[col], col, np.nan)      #returns the col header where True, or np.nan for the False
        group_b_ids      = result
        result.index        = self.date_range
        return group_b_ids




    def create_dry_ids(self):

        # f1  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0, header=0, parse_dates=['datex'])
    
        # self.date_range  = pd.date_range(self.startdate, self.stopdate, freq='D')
        # self.date_range_cols = self.date_range.strftime(self.date_format).to_list()
    
        alive_masknp = self.alive_mask.copy(deep=True).to_numpy()
        # milkers_masknp = self.milkers_mask.to_numpy()
        dry_mask = np.logical_and(alive_masknp, np.logical_not(self.milkers_mask))
        
        dry_count  = dry_mask.sum(axis=1).to_frame(name='dry')
        dry_count.index = self.datex
        dry_count.index.name = 'datex'
        
        dry_ids1 = dry_mask.copy(deep=True)
        for col in dry_mask.columns:
            dry_ids1[col] = np.where(dry_mask[col], col, np.nan)
            
        x               = dry_ids1    
        dry_ids2    = self.create_stack_df(x)
        dry_ids     = pd.DataFrame(dry_ids2, index=[self.datex])

        return  dry_ids, dry_count, dry_mask
    

            

    def allcows_summary(self):

        groups_sum = (self.group_a_count.join(self.group_b_count)).sum(axis=1)
        active_cows = (self.milkers_count.join(self.dry_count)).sum(axis=1)

        self.allcows['group_a_count']    = self.group_a_count
        self.allcows['group_b_count']    = self.group_b_count
        self.allcows['group_sum']        = groups_sum    
        self.allcows['milkers']          = self.milkers_count
        self.allcows['dry_count']        = self.dry_count
        self.allcows['milk+dry']         = active_cows
        self.allcows.index.name          = 'datex'
        return self.allcows



    def create_status_col(self):
        self.wy_series.index 
        
        m1 = self.milkers_mask.iloc[-1,:].copy(deep=True)
        m1.reset_index(drop=True, inplace=True)
        m1.index  += 1
        m2 = pd.DataFrame(self.wy_series[m1])
        m2['status'] = 'M' 
        
        d1 = self.dry_mask.iloc[-1,:]
        d1.reset_index(drop=True, inplace=True)
        d1.index  += 1
        d2 = pd.DataFrame(self.wy_series[d1])
        d2['status'] = 'D' 
        
        g1 = self.gone_mask.iloc[-1,:]
        g1.reset_index(drop=True, inplace=True)
        g1.index  += 1
        g2 = pd.DataFrame(self.wy_series[g1])
        g2['status'] = 'G'
        
        status_col = pd.concat([m2, d2, g2], axis=0)
        status_col.reset_index(drop=True, inplace=True)
        # status_col.sort_index(inplace=True)
        
        return status_col
    
    
    
    
    def create_write_to_csv(self):
        
        self.allcows    .to_csv('F:\\COWS\\data\\status\\allcows.csv')
        self.status_col .to_csv('F:\\COWS\\data\\status\\status_col.csv')
        self.milkers_ids.to_csv('F:\\COWS\\data\\status\\milkers_ids.csv')
        
        self.gone_ids   .to_csv('F:\\COWS\\data\\status\\gone_ids.csv')
        self.alive_ids  .to_csv('F:\\COWS\\data\\status\\alive_ids.csv')
        self.dry_ids    .to_csv('F:\\COWS\\data\\status\\dry_ids.csv')

        self.days_milking_df  .to_csv('F:\\COWS\\data\\status\\days_milking.csv')    
        self.days_mean  .to_csv('F:\\COWS\\data\\status\\days_mean.csv')
        