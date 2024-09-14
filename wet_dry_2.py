'''wet_dry.py'''

import pandas as pd
import numpy as np
from datetime import datetime

from InsemUltraBasics import InsemUltraBasics
# from status_ids import StatusDataLong

# status = StatusDataLong()

today =  pd.Timestamp.today()

stopx    = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  parse_dates=['stop'], header=0)
bd1      = pd.read_csv ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], header=0, index_col='WY_id')
startx   = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'],header=0)

milk1a    = pd.read_csv  ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', parse_dates=['datex'], header=0, index_col=0)

start1a  = startx.pivot (index='WY_id', columns='calf#',    values='b_date')          
stop1a   = stopx .pivot (index='WY_id', columns='lact_num', values='stop')


rng = bd1.index.tolist()

cutoff1 = None     ## !! if eliminating cutoff with None check on col headers and stop2 index reset!!
cutoff2 = None  #263
cutoff3 = None   #-50

# col_head_int =[num  for num in range (cutoff1+1, cutoff2+1)]
# col_head_str =[str(num)  for num in range (cutoff1+1, cutoff2+1)]



# bd2= bd1.iloc[cutoff1:cutoff2,: ].copy()
# bd= bd2.reset_index(drop=True)
# bd['adj_bdate'] = pd.to_datetime(bd['adj_bdate'])

start2a = start1a.reindex(rng)
stop2a  = stop1a .reindex(rng)
start2b = start2a.iloc[cutoff1:cutoff2,:].copy()    #index is WY_ids so this restricts the cow nums
stop2b  = stop2a .iloc[cutoff1:cutoff2,:].copy()

start2= start2b         #.reset_index(drop=True)
stop2= stop2b           #.reset_index(drop=True)

last_start = start2






milk1b = milk1a.iloc[cutoff3:,cutoff1:cutoff2].copy()
milk1c = milk1b         #.reset_index()
# milk1c= milk1c.set_index('datex')
milk1=milk1c
lastday = milk1.index[-1]

# milk1.columns=col_head_str                #no need if milk1c has str headers already

rng_milk = pd.date_range(start='9/1/2016', end= milk1.index[-1])
# milk = milk1.reindex(rng_milk)
milk = milk1

# days_alive = (milk1.index[-1] - bd['adj_bdate'])/np.timedelta64(1,'D')   

cowBdate = bd1['birth_date']
dd = bd1['death_date']
# print('dd   ', bd1.iloc[118:125,:])


class WetDry2:
    def __init__(self):
        
        self.wet_days3 = []
        self.milking_days3 = []
        self.wet_amt3 = []
        self.milking_amt3 = []
        
        [
        self.wet_dict, self.milking_dict,
        self.wet_days3, self.milking_days3, 
        self.wet_amt3, self.milking_amt3
         ]                                  = self.create_wet_milking()

        [
        self.lact1, self.lact2, self.lact3,
        self.lact4, self.lact5, self.lact6,
        self.milking1, self.milking2, 
        self.milking3, self.milking4, 
        self.milking5, self.milking6
        ]                                   = self.create_dataframes()

        (
        self.wet_days, self.wet_amt, 
        self.milking_amt, self.milking_days
        )                                   = self.create_other_dfs()
    
        self.create_write_to_csv()

    def create_wet_milking(self):         

        (   wet_days1,      wet_days2,      wet_days3,  
            milking_days1,  milking_days2,  milking_days3,   
            wet_amt1,       wet_amt2,       wet_amt3,
            milking_amt1,   milking_amt2,   milking_amt3             
        )    =   [],[],[],[],   [],[],[],[],     [],[],[],[]
        
        x=1000
        y= 1  # len(milk1.columns)
        z=0
        rngx = range(1,x,1)
        
        milking1 = np.full((x, y), np.nan)  
        milking2 = np.full((x, y, z), np.nan)  
        milking3 = np.full((x, y, z), np.nan)  
        wet1     = np.full((x, y), np.nan)
        wet2     = np.full((x, y, z), np.nan)
        wet3     = np.full((x, y, z), np.nan)
        
        self.wet_dict = {}
        self.milking_dict = {}
                            
        rows = stop2.index   #list( stop2.index)      #integers
        cols = start2.columns  #integers 
        # milk_cols = list(milk.columns)

       
        for j in cols:  # lact_nums
            for i in rows:         #WY nums
                start       = start2          .loc[i,j]
                stop        = stop2           .loc[i,j]
                dd          = dd[i]
                k           = str(i)
     
                # print(f"xxxxxxxxxx i: {i}, start: {start}, stop: {stop}, dd: {dd}, type(dd): {type(dd)}")
               
                a =  pd.isna(start) is False        # start value exists
                b =  pd.isna(stop)  is False        # stop value exists
                c =  pd.isna(dd)    is False        # is gone  
                d =  pd.isna(dd)    is True         # is alive --dd is blank
                e =  pd.isna(start) is True        # start value missing
                f =  pd.isna(stop)  is True        # stop value missing


                
             
             
            # completed lactation: 
                if a and b:
                    
                    wet_days1=(stop[i]-start[i])/np.timedelta64(1,'D')
                    wet_amt1 = np.nansum(wet1)
                    
                    wet1a = milk.loc[start:stop, k:k]
                    wet1 = wet1a.to_numpy()
                    xpad=x - wet1.shape[0]
                    wet1 = np.pad(wet1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)

                    if wet1.ndim == 2:
                        wet1 = wet1[:,:, np.newaxis]
                        
                    wet2     = np.concatenate((wet2, wet1), axis=2)
                    

            # milking and cow alive
                elif a and d and f:     
                             
                    milking_days1 = (lastday[i]-start[i])/np.timedelta64(1,'D')
                    
                    # note stop date is lastday - cow is still alive
                    milking1c = milk.loc[start:lastday, k:k]
                    
                    milking1 = milking1c.to_numpy()
                    milking_amt1 = np.nansum(milking1)
                    
                    xpad            =  x - milking1.shape[0] 
                    milking1 = np.pad(milking1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)
      
                    if milking1.ndim == 2:
                        milking1 = milking1[:, np.newaxis, :]
                        
                    milking2 = np.concatenate((milking2, milking1), axis=2)

            # milking --  but cow is gone
                elif a and c and f:     

                    milking_days1=(dd[i]-start[i])/np.timedelta64(1,'D')
                    
                    milking1c = milk.loc[start:stop, k:k]

                    milking1 = milking1c.to_numpy()
                    milking_amt1 = np.nansum(milking1)
                    
                    xpad            =  x - milking1.shape[0] 
                    milking1 = np.pad(milking1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)
      
                    if milking1.ndim == 2:
                        milking1 = milking1[:, np.newaxis, :]
                        
                    milking2 = np.concatenate((milking2, milking1), axis=2)
                    
                    
            # everything missing
                elif e and f:
                    pass     
                    

            # iteration end
            
                wet_days2       .append(wet_days1)
                wet_amt2        .append(wet_amt1)
                milking_days2   .append(milking_days1)
                milking_amt2    .append(milking_amt1)
                
            # reinitialize
                wet_days1       = []
                wet_amt1        = []
                milking_days1   = []
                milking_amt1    = []
                wet1     .fill( np.nan)  
                milking1 .fill( np.nan)
                # print('i= ', i ,'j= ', j)


# each lactation iteration finished
            self.wet_days3       .append(wet_days2)
            self.milking_days3   .append(milking_days2)
            self.wet_amt3        .append(wet_amt2)
            self.milking_amt3    .append(milking_amt2)
            

            milking3 = np.concatenate((milking3, milking2), axis=2)
            wet3     = np.concatenate((wet3, wet2), axis=2)
            
            [wet_days2, wet_amt2, milking_days2, milking_amt2] = [], [], [], []
            
            self.wet_dict[j] = wet3
            self.milking_dict[j] = milking3
            # print(self.milking_dict[j])
            wet3        = np.empty((x,y,0))
            milking3    = np.empty((x,y,0))
            
        return [self.wet_dict, self.milking_dict, 
                self.wet_days3, self.milking_days3, 
                self.wet_amt3, self.milking_amt3
                ]
        
             
            

    def create_dataframes(self):
        def reshape_to_dataframe(data):
            
            # print('data: ', data)
        
            if data.size == 0:  # Check if the array is empty
                return pd.DataFrame()
      
            reshaped_data = data.reshape(-1, data.shape[2])   # Combine the first two dimensions and keep the third dimension as columns
            return pd.DataFrame(reshaped_data)

        self.milking1 = reshape_to_dataframe(self.milking_dict[1])
        self.milking2 = reshape_to_dataframe(self.milking_dict[2])
        self.milking3 = reshape_to_dataframe(self.milking_dict[3])
        self.milking4 = reshape_to_dataframe(self.milking_dict[4])
        self.milking5 = reshape_to_dataframe(self.milking_dict[5])
        self.milking6 = reshape_to_dataframe(self.milking_dict[6])

        self.lact1 = reshape_to_dataframe(self.wet_dict[1])
        self.lact2 = reshape_to_dataframe(self.wet_dict[2])
        self.lact3 = reshape_to_dataframe(self.wet_dict[3])
        self.lact4 = reshape_to_dataframe(self.wet_dict[4])
        self.lact5 = reshape_to_dataframe(self.wet_dict[5])
        self.lact6 = reshape_to_dataframe(self.wet_dict[6])

        return [
            self.lact1, self.lact2, self.lact3,
            self.lact4, self.lact5, self.lact6,
            self.milking1, self.milking2, self.milking3,
            self.milking4, self.milking5, self.milking6
        ]
        
    def create_other_dfs(self):
        
        self.wet_days       = pd.DataFrame(self.wet_days3)
        self.wet_amt        = pd.DataFrame(self.wet_amt3)
        self.milking_days   = pd.DataFrame(self.milking_days3) 
        self.milking_amt    = pd.DataFrame(self.milking_amt3) 

        # self.wet_days_sum       = self.wet_days     .sum(axis=1)
        # self.wet_amt_sum        = self.wet_amt      .sum(axis=1)
        # self.milking_days_sum   = self.milking_days .sum(axis=1)
        # self.milking_amt_sum    = self.milking_amt  .sum(axis=1)
        
        return (self.wet_days, self.wet_amt, 
                self.milking_amt, self.milking_days
                )
    
                
    def create_write_to_csv(self):      
        self.wet_days            .to_csv('F:\\COWS\\data\\milk_data\\lactations\\wet_days.csv')
        self.wet_amt             .to_csv('F:\\COWS\\data\\milk_data\\lactations\\wet_amt.csv')
        self.milking_amt         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_amt.csv')   
        self.milking_days        .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_days.csv')   
        
        self.lact1         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_1.csv')
        self.lact2         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_2.csv')
        self.lact3         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_3.csv')
        self.lact4         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_4.csv')
        self.lact5         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_5.csv')
        self.lact6         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_6.csv')
        
        self.milking1         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_1.csv')
        self.milking2         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_2.csv')
        self.milking3         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_3.csv')
        self.milking4         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_4.csv')
        self.milking5         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_5.csv')
        self.milking6         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_6.csv')
