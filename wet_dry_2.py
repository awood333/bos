'''wet_dry.py'''

import pandas as pd
import numpy as np
# from datetime import datetime as dt
from datetime import datetime

# from status_ids import StatusDataLong

# status = StatusDataLong()

today =  pd.Timestamp.today()

lb      = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'], header=0)
stopx    = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  parse_dates=['stop'], header=0)
bd1      = pd.read_csv ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], header=0, index_col='WY_id')
startx   = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'],header=0)

milk1a    = pd.read_csv  ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', parse_dates=['datex'], header=0, index_col=0)

start1a  = startx.pivot (index='WY_id', columns='calf#',    values='b_date')          
stop1a   = stopx .pivot (index='WY_id', columns='lact_num', values='stop')
lbpivot1     = lb   .pivot (index='WY_id', columns='calf#',    values='b_date')

not_heifers1 = lbpivot1.index.values.tolist()  #eliminate heifers - only contains cows that have calved

rng = bd1.index.tolist()

cutoff1 = 259 #None
cutoff2 = 263
cutoff3 = None #-200

lastday = milk1a.index[-1]

bd2= bd1.iloc[cutoff1:cutoff2,: ].copy()
bd= bd2.reset_index(drop=True)
bd['adj_bdate'] = pd.to_datetime(bd['adj_bdate'])

start2a = start1a.reindex(rng)
stop2a  = stop1a .reindex(rng)
start2b=start2a.iloc[cutoff1:cutoff2,: ].copy()
stop2b=stop2a.iloc[cutoff1:cutoff2,:].copy()

start2= start2b.reset_index(drop=True)
stop2= stop2b.reset_index(drop=True)

# new_colheaders = ['1','2','3','4','5']
milk1b = milk1a.iloc[cutoff3:,cutoff1:cutoff2].copy()
milk1c = milk1b.reset_index()
milk1c= milk1c.set_index('datex')
milk1=milk1c
# milk1.columns=new_colheaders

rng_milk = pd.date_range(start='9/1/2016', end= milk1.index[-1])
# milk = milk1.reindex(rng_milk)
milk = milk1

# days_alive = (milk1.index[-1] - bd['adj_bdate'])/np.timedelta64(1,'D')   

cowBdate = bd['birth_date']
Ddate = bd['death_date']


class WetDry2:
    def __init__(self):
        [self.wet_days, self.wet_amt, 
            self.milking_amt, self.milking_days,
            self.wet_series]   = self.create_wet_milking()
        self.lact1
        self.lact2
    


    def create_wet_milking(self):         

        (   wet_days1,      wet_days2,      wet_days3,  
            milking_days1,  milking_days2,  milking_days3,   
            wet_amt1,       wet_amt2,       wet_amt3,
            milking_amt1,   milking_amt2,   milking_amt3             
        )    =   [],[],[],[],   [],[],[],[],     [],[],[],[]
        
        x=100
        y= 1  # len(milk1.columns)
        z=0
        
        milking_series1 = np.full((x, y), np.nan)  
        milking_series2 = np.full((x, y, z), np.nan)  
        milking_series3 = np.full((x, y, z), np.nan)  
        wet_series1     = np.full((x, y), np.nan)
        wet_series2     = np.full((x, y, z), np.nan)
        wet_series3     = np.full((x, y, z), np.nan)
         
        rows = list( stop2.index)      #integers
        cols = start2.columns  #integers 
        rngx = range(1,x,1)
       
        for j in cols:  # lact_nums
            for i in rows:         #WY nums
                start       = start2          .loc[i,j]
                stop        = stop2           .loc[i,j]
                dd          = bd['death_date'][i]
                k           = str(j)
     
               
                a =  pd.isna(start) is False        # start value exists
                b =  pd.isna(stop)  is False        # stop value exists
                c =  pd.isna(dd)    is False        # is gone  
                d =  pd.isna(dd)    is True         # is alive --dd is blank
                e =  pd.isna(start) is True        # start value missing
                f =  pd.isna(stop)  is True        # stop value missing
                g = stop > Ddate
   
            
            # completed lactation:   cow not dead  (dd blank)    
                if a and b and c:
                    
                    wet_days1=(stop-start)/np.timedelta64(1,'D')
                    wet_amt1 = np.nansum(wet_series1)
                    
                    wet_series_1a = milk.loc[start:stop, k:k]
                    wet_series1 = wet_series_1a.to_numpy()
                    xpad=x - wet_series1.shape[0]
                    wet_series1 = np.pad(wet_series1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)

                    if wet_series1.ndim == 2:
                        wet_series1 = wet_series1[:,:, np.newaxis]
                        
                    wet_series2     = np.concatenate((wet_series2, wet_series1), axis=2)
                    
                    
            # completed lactation,  but cow gone (dd not blank)
                elif a and  b and d :   
                    
                    wet_days1=(stop-start)/np.timedelta64(1,'D')      
                    wet_amt1 = np.nansum(wet_series1)
                    
                    wet_series_1a = milk.loc[start:stop, k:k]
                    wet_series1 = wet_series_1a.to_numpy()
                    xpad=x - wet_series1.shape[0]
                    wet_series1 = np.pad(wet_series1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)
                    
                    if wet_series1.ndim == 2:
                        wet_series1 = wet_series1[:, np.newaxis]

                    wet_series2     = np.concatenate((wet_series2, wet_series1), axis=2)
                    
                    
            # milking --  cow is alive
                elif a and d and f:     
                    
                    if stop > Ddate and a :
                        st
                             
                    milking_days1 = (stop-start)/np.timedelta64(1,'D')
                    milking_amt1 = np.nansum(milking_series1)
                    
                    
                    # note stop date is lastday - cow is still alive
                    milking_series1a = milk.loc[start:lastday, k:k]
                    
                    print(milking_series1a)
                    milking_series1 = milking_series1a.to_numpy()
                    xpad            =  x - milking_series1.shape[0] 
                    milking_series1 = np.pad(milking_series1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)
      
                    if milking_series1.ndim == 2:
                        milking_series1 = milking_series1[:, np.newaxis, :]
                        
                    milking_series2 = np.concatenate((milking_series2, milking_series1), axis=2)



            # milking --  but cow is gone
                elif a and c and f:     
                    
                                     
                    milking_days1=(stop-start)/np.timedelta64(1,'D')
                    
                    
                    milking_series_1a = milk.loc[start:Ddate, k:k]
                    milking_amt1 = np.nansum(milking_series1)
                    milking_series1 = milking_series1a.to_numpy()
                    xpad            =  x - milking_series1.shape[0] 
                    milking_series1 = np.pad(milking_series1, ((0, xpad), (0, 0)), 'constant', constant_values=np.nan)
      
                    if milking_series1.ndim == 2:
                        milking_series1 = milking_series1[:, np.newaxis, :]
                        
                    milking_series2 = np.concatenate((milking_series2, milking_series1), axis=2)
                    

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
                wet_series1     .fill( np.nan)  
                milking_series1 .fill( np.nan)
                print('i= ', i ,'j= ', j)


# all iterations finished
            wet_days3       .append(wet_days2)
            milking_days3   .append(milking_days2)
            wet_amt3        .append(wet_amt2)
            milking_amt3    .append(milking_amt2)
            
            wet_series3     = np.concatenate(
                (wet_series3, wet_series2), axis=2)
            
            milking_series3 = np.concatenate(
                (milking_series3, milking_series2), axis=2)
      
            # z += 1
                
            # wet_series2     = np.pad(wet_series2,       ((0, 0), (0, 0), (0, 1)), 'constant', constant_values=np.nan)
            # milking_series2 = np.pad(milking_series2,   ((0, 0), (0, 0), (0, 1)), 'constant', constant_values=np.nan)
            
            [wet_days2, wet_amt2, milking_days2, milking_amt2] = [], [], [], []
             
            # print(wet_series3[:, :, 0])
        self.lact1 = pd.DataFrame(wet_series3[:, :, 0])
        self.lact2 = pd.DataFrame(wet_series3[:, :, 1])  
           
           

        self.wet_days           = pd.DataFrame(wet_days3, columns=rows)
        self.wet_days               .replace('', np.nan, inplace=True)
        self.wet_days           = self.wet_days.astype(float) 
       
        
        self.wet_amt        = pd.DataFrame(wet_amt3, columns=rows)
        self.wet_amt            .replace('', np.nan, inplace=True)
        self.wet_amt        = self.wet_amt.astype(float) 

        self.milking_days       = pd.DataFrame(milking_days3, columns=rows) 
        self.milking_days       .replace('', np.nan, inplace=True)
        self.milking_days       = self.milking_days.astype(float) 
       
        self.milking_amt    = pd.DataFrame(milking_amt3, columns=rows) 
        self.milking_amt        .replace('', np.nan, inplace=True)
        self.milking_amt    = self.milking_amt.astype(float) 

        self.milking_series    = pd.DataFrame(milking_series3, columns=rows) 
        self.milking_series        .replace('', np.nan, inplace=True)
        self.milking_series    = self.milking_series.astype(float) 


        self.wet_days_sum       = self.wet_days.sum(axis=1)
        self.wet_amt_sum        = self.wet_amt.sum(axis=1)
        self.milking_days_sum   = self.milking_days.sum(axis=1)
        self.milking_amt_sum    = self.milking_amt.sum(axis=1)
        
        return (  self.wet_days, self.wet_amt, 
                self.milking_amt, self.milking_days,
                self.wet_series)
    
                
    def create_write_to_csv(self):      
        self.wet_days             .to_csv('F:\\COWS\\data\\wet_dry\\wet_days.csv')
        self.wet_amt             .to_csv('F:\\COWS\\data\\wet_dry\\wet_amt.csv')
        self.milking_amt            .to_csv('F:\\COWS\\data\\wet_dry\\milking_amt.csv')   
        self.milking_days        .to_csv('F:\\COWS\\data\\wet_dry\\milking_days.csv')   
        # self.wet_series         .to_csv('F:\\COWS\\data\\wet_dry\\wet_series.csv')  
  

# def main():
#     wet_dry = WetDry2()
#     wet_dry.create_write_to_csv()


# if __name__ == "__main__":
#     main()


