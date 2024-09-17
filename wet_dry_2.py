'''wet_dry.py'''

import pandas as pd
import numpy as np
from datetime import datetime

from InsemUltraBasics import InsemUltraBasics
# from status_ids import StatusDataLong

# status = StatusDataLong()

today =  pd.Timestamp.today()
class WetDryBasics:
    def __init__(self):
        
        IUB = InsemUltraBasics()

        stopx    = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  parse_dates=['stop'], header=0)
        bd1      = pd.read_csv ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], header=0, index_col='WY_id')
        startx   = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'],header=0)
        milk1a    = pd.read_csv  ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', parse_dates=['datex'], header=0, index_col=0)

        start1a  = startx.pivot (index='WY_id', columns='calf#',    values='b_date')          
        stop1a   = stopx .pivot (index='WY_id', columns='lact_num', values='stop')

        rng = bd1.index.tolist()

        self.cutoff1 = 255 #None     ## !! if eliminating cutoff with None check on col headers and stop2 index reset!!
        self.cutoff2 = 260 #None  #263
        self.cutoff3 = -500 #None   #-50
        
        self.milk   = milk1a.iloc[self.cutoff3:,self.cutoff1:self.cutoff2].copy()
      
        self.lastday= self.milk.index[-1]

        self.extended_rng_milk = pd.date_range(start='9/1/2016', end= self.milk.index[-1])
      

        # col_head_int =[num  for num in range (cutoff1+1, cutoff2+1)]
        # col_head_str =[str(num)  for num in range (cutoff1+1, cutoff2+1)]

        start2a = start1a.reindex(rng)
        stop2a  = stop1a .reindex(rng)
        
        #index is WY_ids so this restricts the cow nums same as in milk1b
        start2b = start2a.iloc[self.cutoff1:self.cutoff2,:].copy()    
        stop2b  = stop2a .iloc[self.cutoff1:self.cutoff2,:].copy()

        self.start2 = start2b
        self.stop2  = stop2b
        
        self.last_start = IUB.last_calf
        self.last_stop  = IUB.last_stop

        self.dd = bd1['death_date']



class WetDry2:
    def __init__(self):
        
        self.WDB = WetDryBasics()
        
        self.wet_days3 = []
        self.milking_days3 = []
        self.wet_amt3 = []
        self.milking_amt3 = []

        self.wet_dict = {}  # Initialize wet_dict
        self.milking_dict = {}  # Initialize milking_dict
                
        [self.wet_i_3, self.milking_i_3] = [],[]
        
        
        [
        self.wet_dict, self.milking_dict,
        self.wet_days3, self.milking_days3, 
        self.wet_amt3, self.milking_amt3,
        self.wet3, self.milking3
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
            milking_amt1,   milking_amt2,   milking_amt3,
            milking_i_1,    blank_cols_i_1, wet_i_1,
            milking_i_2,    blank_cols_i_2, wet_i_2,
            milking_i_3,    blank_cols_i_3, wet_i_3
              
        )    =   [],[],[],  [],[],[],  [],[],[],  [],[],[],  [],[],[], [],[],[], [],[],[]
        
        x=1000
        y= 1  # len(self.milk.columns)
        z=0
        
        milking1 = np.full((x, y), np.nan)  
        milking2 = np.full((x, y, z), np.nan)  
        self.milking3 = np.full((x, y, z), np.nan)  
        wet1     = np.full((x, y), np.nan)
        wet2     = np.full((x, y, z), np.nan)
        self.wet3     = np.full((x, y, z), np.nan)
        
        self.wet_dict = {}
        self.milking_dict = {}
        
        dd      = self.WDB.dd
                            
        rows = self.WDB.stop2.index   #list( stop2.index)      #integers
        cols = self.WDB.start2.columns  #integers 

       
        for j in cols:  # lact_nums
            for i in rows:         #WY nums
                
                start   = self.WDB.start2.loc[i,j]
                stop    = self.WDB.stop2.loc[i,j]
                lastday = self.WDB.lastday                #last day of the milk df datex
                k           = str(i)

                a =  pd.isna(start) is False        # start value exists
                b =  pd.isna(stop)  is False        # stop value exists
                c =  pd.isna(dd)    is False        # is gone  
                d =  pd.isna(dd)    is True         # is alive --dd is blank
                e =  pd.isna(start) is True        # start value missing
                f =  pd.isna(stop)  is True        # stop value missing

             
                # completed lactation: 
                if a and b:
                    
                    wet_days1=(stop - start)/np.timedelta64(1,'D')
                    wet1a   = self.WDB.milk.loc[start:stop, k:k]
                    wet1    = wet1a.to_numpy()
                    wet_amt1 = np.nansum(wet1)
                    xpad    = x - wet1.shape[0]
                    wet1    = np.pad(wet1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)

                    if wet1.ndim == 2:
                        wet1 = wet1[:,:, np.newaxis]
                        
                    wet2     = np.concatenate((wet2, wet1), axis=2)
                    wet_i_1.append(i)
                    
                # milking 
                elif a and f:     

                    milking_days1 = (lastday-start)/np.timedelta64(1,'D')
                    milking1a = self.WDB.milk.loc[start:lastday, k:k]               
                    milking1 = milking1a.to_numpy()
                    milking_amt1 = np.nansum(milking1)
                    xpad            =  x - milking1.shape[0] 
                    milking1 = np.pad(milking1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)
      
                    if milking1.ndim == 2:
                        milking1 = milking1[:, np.newaxis, :]
                        
                    milking2 = np.concatenate((milking2, milking1), axis=2)
                    milking_i_1.append(i)
                    
                # everything missing
                elif e and f:
                    blank_cols_i_1.append(i)     
                    
                # iteration end
                wet_days2       .append(wet_days1)
                wet_amt2        .append(wet_amt1)
                milking_days2   .append(milking_days1)
                milking_amt2    .append(milking_amt1)
                
                wet_i_2         .append(wet_i_1)
                milking_i_2     .append(milking_i_1)
                blank_cols_i_2  .append(blank_cols_i_1)

                # reinitialize
                [wet_days1,     wet_amt1]           = [],[]
                [milking_days1, milking_amt1]       = [],[]
                [ milking_i_1,  blank_cols_i_1 ]    = [],[]
                wet_i_1                           = []
                
                wet1     .fill( np.nan)  
                milking1 .fill( np.nan)


# each lactation iteration finished
            self.wet_days3      .append(wet_days2)
            self.milking_days3  .append(milking_days2)
            self.wet_amt3       .append(milking_days2)
            self.milking_amt3   .append(milking_amt2)
            self.wet_i_3        .append(wet_i_2)       
            self.milking_i_3    .append(milking_i_2)
    


            if hasattr(self, 'wet3'):
                self.wet3 = np.concatenate((self.wet3, wet2), axis=2)
                self.wet_dict[j] = self.wet3
                print(f'wet_dict {j}',self.wet_dict[j])
            else:
                self.wet3=wet2
                
            if hasattr(self, 'milking3'):
                self.milking3 = np.concatenate((self.milking3, milking2), axis=2)    
                self.milking_dict[j] = self.milking3
                print(f'milking_dict {j}.shape',self.milking_dict[j].shape,self.milking_dict[j])
            else:
                self.milking3=milking2

            [wet_days2,     wet_amt2,       milking_days2]  = [],[],[]
            [milking_amt2,  milking_i_2,    blank_cols_i_2] = [],[],[] 
            wet_i_2                                       = []
                
        return [self.wet_dict, self.milking_dict, 
                    self.wet_days3, self.milking_days3, 
                    self.wet_amt3, self.milking_amt3,
                    self.wet3, self.milking3
                    ]


    def create_dataframes(self):

        self.milking_1 = pd.DataFrame(self.milking_dict[1][:,0,:])
        self.milking_2 = pd.DataFrame(self.milking_dict[2][:,0,:])
        self.milking_3 = pd.DataFrame(self.milking_dict[3][:,0,:])
        self.milking_4 = pd.DataFrame(self.milking_dict[4][:,0,:])
        self.milking_5 = pd.DataFrame(self.milking_dict[5][:,0,:])
        self.milking_6 = pd.DataFrame(self.milking_dict[6][:,0,:])

        self.lact_1 = pd.DataFrame(self.wet_dict[1][:,0,:])
        self.lact_2 = pd.DataFrame(self.wet_dict[2][:,0,:])
        self.lact_3 = pd.DataFrame(self.wet_dict[3][:,0,:])
        self.lact_4 = pd.DataFrame(self.wet_dict[4][:,0,:])
        self.lact_5 = pd.DataFrame(self.wet_dict[5][:,0,:])
        self.lact_6 = pd.DataFrame(self.wet_dict[6][:,0,:])

        return [
            self.lact_1, self.lact_2, self.lact_3,
            self.lact_4, self.lact_5, self.lact_6,
            self.milking_1, self.milking_2, self.milking_3,
            self.milking_4, self.milking_5, self.milking_6
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
        
        self.lact_1         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_1.csv')
        self.lact_2         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_2.csv')
        self.lact_3         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_3.csv')
        self.lact_4         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_4.csv')
        self.lact_5         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_5.csv')
        self.lact_6         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_6.csv')
        
        self.milking_1         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_1.csv')
        self.milking_2         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_2.csv')
        self.milking_3         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_3.csv')
        self.milking_4         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_4.csv')
        self.milking_5         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_5.csv')
        self.milking_6         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_6.csv')
