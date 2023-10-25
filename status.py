'''
status.py
'''
import pandas as pd
import numpy as np
from startdate_funct import create_startdate

class StatusData:
    def __init__(self):
        self.f1             = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',
                                    index_col=0,  header=0, parse_dates=['datex'])
        self.bd        = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv', index_col=0, header=0,
                                    parse_dates=['birth_date','death_date','arrived', 'adj_bdate'])
        self.lb_last   = pd.read_csv('F:\\COWS\\data\\insem_data\\lb_last.csv',
                                    parse_dates=['lastcalf bdate'], index_col=0, header=0)
        
        self.maxdate        = self.f1.index.max() 
        self.stopdate  = self.maxdate 
        self.bdmax          = len(self.bd)
        self.startdate,  self.date_format = create_startdate()

        self.date_range = pd.date_range(self.startdate, self.stopdate, freq='D')
        self.f              = self.f1.loc[self.date_range, :]       
        self.date_range_cols = self.date_range.strftime(self.date_format).to_list()
        
        self.alive_mask, self.alive_count_df, self.gone_mask, self.gone_count_df,self.nby_count_df,   self.ungone, self.allcows = self.create_alive_mask()
        self.alive_ids_df = self.create_alive_ids()
        self.milkers_count_df, self.milkers_mask, self.milkers_mask_df    = self.create_milkers_mask()
        self.milkers_ids    = self.create_milkers_ids()
        
        self.days_milking_df= self.create_days()
        self.group_a ,  self.group_a_count          = self.create_group_a()
        self.group_b ,  self.group_b_count          = self.create_group_b()
        
        self.group_a_ids = self.create_group_a_ids()
        self.group_b_ids = self.create_group_b_ids()
        self.dry_ids_df, self.dry_count_df          = self.create_dry_ids()

        

    def create_date_range(self):
        self.date_range      = pd.date_range(self.startdate, self.stopdat, freq='D')
        self.date_range_cols = self.date_range.strftime(self.date_format).to_list()
        return self.date_range, self.date_range_cols



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
                (var2 <= self.date_range) 
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
        alive_ids_df = self.alive_mask.copy()                
        for col in self.alive_mask.copy().columns: 
            alive_ids_df[col] = np.where(self.alive_mask[col], col, np.nan)

        return alive_ids_df         # duplicates the mask, but with the wy_ids in each cell

    # @property
    # def alive_ids_df(self):
    #     alive_ids_df = self.create_alive_ids()
    #     return alive_ids_df

    def create_milkers_mask(self):
        milkers_mask        = self.f > 0   #if it's recorded as milking that day  TF
        milkers_mask_df     = pd.DataFrame(milkers_mask, columns = self.f.columns, index=self.date_range)
        milkers_count       = milkers_mask.sum(axis=1).to_frame()              #count of True in each row (each date)
        milkers_count_df    = pd.DataFrame(milkers_count, index=self.date_range)
        return milkers_mask, milkers_mask_df, milkers_count_df



    def create_milkers_ids(self):      
        result = self.milkers_mask
        for col in self.milkers_mask.columns:
            result[col] = np.where(self.milkers_mask[col], col, np.nan)      #returns the col header where True, or np.nan for the False
        milkers_ids_df = pd.DataFrame(result, columns=self.milkers_mask.columns, index=self.date_range_cols)
        return milkers_ids_df
    
    # @property
    # def milkers_ids_df(self):
    #     milkers_ids_df = self.create_milkers_ids()
    #     return milkers_ids_df



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
                        lastcalf_bdate  = self.lb_last.loc[int(col), 'lastcalf bdate'  ]
                        date_diff       = (index - lastcalf_bdate).days
                        days_result[col]    = date_diff
            days1.append(days_result)
        days_milking_df = pd.DataFrame(days1, index=self.date_range_cols)
        return days_milking_df  



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
        group_a_ids_df      = result #pd.DataFrame(result, columns=group_a.columns, index=self.date_range_cols)
        return group_a_ids_df





    def create_group_b_ids(self):     

        result              = self.group_b
        for col in self.group_b.columns:
            result[col]     = np.where(self.group_b[col], col, np.nan)      #returns the col header where True, or np.nan for the False
        group_b_ids_df      = result
        result.index        = self.date_range
        return group_b_ids_df


    def create_dry_ids(self):

        # f1  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', index_col=0, header=0, parse_dates=['datex'])
    
        self.date_range  = pd.date_range(self.startdate, self.stopdate, freq='D')
        self.date_range_cols = self.date_range.strftime(self.date_format).to_list()
    

        milkers_mask        = self.f > 0  
        alive_masknp = self.alive_mask.copy().to_numpy()
        milkers_masknp = milkers_mask.to_numpy()
        dry_mask = np.logical_and(alive_masknp, np.logical_not(milkers_mask))
        
        result = dry_mask.copy()
        for col in dry_mask.columns:
            result[col] = np.where(dry_mask[col], col, np.nan)
            
        dry_ids         = result
        dry_ids_df      = pd.DataFrame(result, columns=dry_mask.columns, index=self.date_range_cols)
        dry_count_df       = dry_mask.sum(axis=1).to_frame(name='dry')
        dry_count_df.index = self.date_range
        dry_count_df.index.name = 'datex'
        return  dry_ids_df, dry_count_df


    def allcows_summary(self):

        groups_sum = (self.group_a_count.join(self.group_b_count)).sum(axis=1)
        active_cows = (self.milkers_count_df.join(self.dry_count_df)).sum(axis=1)

        self.allcows['group_a_count']    = self.group_a_count
        self.allcows['group_b_count']    = self.group_b_count
        self.allcows['group_sum']        = groups_sum    
        self.allcows['milkers']          = self.milkers_count_df
        self.allcows['dry_count']        = self.dry_count_df
        self.allcows['milk+dry']         = active_cows
        self.allcows.index.name          = 'datex'
        return self.allcows

    # @property
    # def allcows(self):
    #     self.check_group_sums()
    #     return self.allcows
            

#     def process_data(self):
        
#         # date_range, date_range_cols = create_date_range(startdate, stopdate, date_format)
#         alive_mask, alive_count_df, gone_mask, gone_count_df,  nby_count_df,  ungone, allcows   = self.create_alive_mask()
#         alive_ids_df                = self.create_alive_ids(alive_mask)

#         milkers_mask, milkers_mask_df, milkers_count_df = self.create_milkers_mask()
#         days_milking_df             = self.create_days(milkers_mask)
#         milkers_ids                 = self.create_milkers_ids(milkers_mask)

#         group_a, group_a_count      = self.create_group_a(days_milking_df)
#         group_b, group_b_count      = self.create_group_b(days_milking_df)

#         group_a_ids_df              = self.create_group_a_ids(group_a)
#         group_b_ids_df              = self.create_group_b_ids(group_b)

#         dry_ids_df, dry_count_df    = self.create_dry_ids(alive_mask)
#         allcows                     = self.check_group_sums(allcows, group_a_count, group_b_count, milkers_count_df, dry_count_df)


#         return {
#             "self.date_range": self.date_range,
#             "self.date_range_cols": self.date_range_cols,
#             "alive_mask": alive_mask,
#             "gone_mask": gone_mask,
#             "nby_count_df": nby_count_df,
#             "ungone": ungone,
#             "allcows": allcows,
#             "alive_ids_df": alive_ids_df,
#             "milkers_mask": milkers_mask,
#             "milkers_mask_df": milkers_mask_df,
#             "milkers_count_df": milkers_count_df,
#             "days_milking_df": days_milking_df,
#             "milkers_ids": milkers_ids,
#             "group_a": group_a,
#             "group_a_count": group_a_count,
#             "group_b": group_b,
#             "group_b_count": group_b_count,
#             "group_a_ids_df": group_a_ids_df,
#             "group_b_ids_df": group_b_ids_df,
#             "dry_ids_df": dry_ids_df,
#             "dry_count_df": dry_count_df,
#         }


# if __name__ == '__main__':
#     processor = StatusData()
#     x=1
#     print('processor up')
#     data  = processor.process_data()
    


