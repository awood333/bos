'''wet_dry.py'''

import pandas as pd
import numpy as np

from milk_functions.WetDryBasics import WetDryBasics

today =  pd.Timestamp.today()

class WetDry:
    def __init__(self):
        
        self.WDB = WetDryBasics()
        
        self.wet_days3 = []
        self.milking_days3 = []
        self.wet_sum3 = []
        self.milking_sum3 = []
        self.wet_mean3 = [] 
        self.milking_mean3 = [] 
        self.wet_max3 = []
        self.milking_max3 = []

        self.wet_dict = {}
        self.milking_dict = {}
                
        self.wet_header     = []
        self.milking_header = []
        
        [self.wet_dict, self.milking_dict, 
        self.wet_days3, self.milking_days3, 
        self.wet_sum3,  self.milking_sum3,
        self.wet_mean3, self.milking_mean3,
        self.wet_max3,  self.milking_max3,
        self.wet_header, self.milking_header
        ]                                  = self.create_wet_milking()

        [
        self.lact1,     self.lact2,         self.lact3,
        self.lact4,     self.lact5,         self.lact6,
        self.milking1,  self.milking2, 
        self.milking3,  self.milking4, 
        self.milking5,  self.milking6,
        self.milking
        ]                                   = self.create_dataframes()

        (self.wet_days, self.wet_sum, 
         self.wet_mean, self.wet_max,
         self.milking_sum, self.milking_days, 
         self.milking_mean, self.milking_max
                )                             = self.create_other_dfs()
    
        self.create_write_to_csv()



    def create_wet_milking(self):         

        (   wet_days1,      wet_days2,      
            wet_sum1,       wet_sum2,       
            wet_mean1,      wet_mean2,      
            wet_max1,       wet_max2,       
            wet_i_1,        wet_i_2   
            ) =[],[],[],[],[],  [],[],[],[],[]
            
        (   milking_days1,  milking_days2,  
            milking_sum1,   milking_sum2, 
            milking_mean1,  milking_mean2,
            milking_max1,   milking_max2,
            milking_i_1,    milking_i_2
            ) =[],[],[],[], [],[],[],[],[],[]
            
        (   blank_cols_i_1, blank_cols_i_2)    =   [],[]
        

        
        x=1000
        y= 1  
        z=0
        
        milking1 = np.full((x, y), np.nan)  
        milking2 = np.full((x, y, z), np.nan)  
        milking3 = np.full((x, y, z), np.nan)  
        wet1     = np.full((x, y), np.nan)
        wet2     = np.full((x, y, z), np.nan)
        wet3     = np.full((x, y, z), np.nan)
        
        self.wet_dict = {}
        self.milking_dict = {}
        
        # dd      = self.WDB.dd
                            
        rows = self.WDB.stop2.index   #list( stop2.index)      #integers
        cols = self.WDB.start2.columns  #integers 

       
        for j in cols:  # lact_nums
            for i in rows:         #WY nums
                
                start   = self.WDB.start2.loc[i,j]
                stop    = self.WDB.stop2.loc[i,j]
                lastday = self.WDB.lastday                #last day of the milk df datex
                k       = str(i)
                w       = j-1    

                a =  pd.isna(start) is False        # start value exists
                b =  pd.isna(stop)  is False        # stop value exists
                # c =  pd.isna(dd)    is False        # is gone  
                # d =  pd.isna(dd)    is True         # is alive --dd is blank
                e =  pd.isna(start) is True        # start value missing
                f =  pd.isna(stop)  is True        # stop value missing

             
                # completed lactation: 
                if a and b:
                    
                    wet_days1=(stop - start)/np.timedelta64(1,'D')
                    wet1a   = self.WDB.milk.loc[start:stop, k:k]
                    wet1    = wet1a.to_numpy().astype(float)
                    wet_sum1 = np.nansum(wet1)
                    
                     
                    if wet1.size > 0:
                        wet_max1 = np.nan if np.isnan(wet1).all() else np.nanmax(wet1)
                        wet_mean1 = np.nan if np.isnan(wet1).all() else np.nanmean(wet1)
                    else:
                        wet_max1 = np.nan
                        wet_mean1 = np.nan
                    
                    xpad    = x - wet1.shape[0]
                    wet1    = np.pad(wet1, ((0, xpad), (0, 0)), 'constant', constant_values=0)

                    if wet1.ndim == 2:
                        wet1 = wet1[:,:, np.newaxis]
                        
                    wet2     = np.concatenate((wet2, wet1), axis=2)
                    wet_i_1.append(i)
                    
                # milking 
                elif a and f:     

                    milking_days1 = (lastday-start)/np.timedelta64(1,'D')
                    milking1a = self.WDB.milk.loc[start:lastday, k:k]               
                    milking1 = milking1a.to_numpy()
                    milking_sum1 = np.nansum(milking1)
                    milking_mean1 = np.nanmean(milking1)
                    
                    if milking1.size>0:
                        milking_max1  = np.nanmax(milking1)
                        milking_mean1 = np.nanmean(milking1)
                    else:
                        milking_max1  = np.nan
                        milking_mean1 = np.nan
                    
                    xpad            =  x - milking1.shape[0] 
                    milking1 = np.pad(milking1, ((0, xpad), (0, 0)), 'constant', constant_values=0)
      
                    if milking1.ndim == 2:
                        milking1 = milking1[:, np.newaxis, :]
                        
                    milking2 = np.concatenate((milking2, milking1), axis=2)
                
                    milking_i_1.append(i)
                    
                # everything missing
                elif e and f:
                    blank_cols_i_1.append(i)     
                    
                # iteration end
                wet_days2       .append(wet_days1)
                wet_sum2        .append(wet_sum1)
                wet_mean2       .append(wet_mean1)
                wet_max2        .append(wet_max1)
                
                
                milking_days2   .append(milking_days1)
                milking_sum2    .append(milking_sum1)
                milking_mean2   .append(milking_mean1)
                milking_max2    .append(milking_max1)
                
                
                if wet_i_1:
                    wet_i_2     .append(wet_i_1)
                    
                if milking_i_1:
                    milking_i_2 .append(milking_i_1)
                    
                if blank_cols_i_1:
                    blank_cols_i_2  .append(blank_cols_i_1)
                

                # reinitialize
                [wet_days1,     wet_sum1,
                 wet_mean1,      wet_max1,
                 wet_i_1]                    = [],[],[],[],[]

                [milking_days1, milking_sum1,
                 milking_mean1, milking_max1,
                 milking_i_1]               = [],[],[],[],[]
                
                blank_cols_i_1            = []
                               
                
                wet1     .fill( 0)  
                milking1 .fill( 0)


# each lactation iteration finished
            self.wet_days3      .append(wet_days2)
            self.wet_sum3       .append(wet_sum2)
            self.wet_mean3      .append(wet_mean2)
            self.wet_max3       .append(wet_max2)
            self.wet_header     .append(wet_i_2)
             
            self.milking_days3  .append(milking_days2)
            self.milking_sum3   .append(milking_sum2)
            self.milking_mean3  .append(milking_mean2)
            self.milking_max3   .append(milking_max2)
            self.milking_header .append(milking_i_2)
            

            if wet2.shape[2] > 0:
                wet3 = np.concatenate((wet3, wet2), axis=2)
            else: wet3 = wet2
            self.wet_dict[j] = wet3    
           
            if  milking2.shape[2] > 0:
                milking3 = np.concatenate((milking3, milking2), axis=2)
            else:milking3 = milking2
            self.milking_dict[j] = milking3
         
           
            [wet_days2,     wet_sum2,   wet_mean2,
             wet_max2,   wet_i_2]   =      [],[],[], [],[] 

            [milking_days2,   milking_sum2,  milking_mean2,
             milking_max2,    milking_i_2]    =      [],[],[], [],[] 
             
            blank_cols_i_2 = []
                                     
            
            wet2        = np.empty((x,y,z))
            wet3        = np.empty((x,y,z))
            milking2    = np.empty((x,y,z))
            milking3    = np.empty((x,y,z))
            
            # for w in self.milking_dict:
            #     self.milking_dict[w] = self.milking_dict[w].reshape(-1)
            
                
        return [self.wet_dict,      self.milking_dict, 
                    self.wet_days3, self.milking_days3, 
                    self.wet_sum3,  self.milking_sum3,
                    self.wet_mean3, self.milking_mean3,
                    self.wet_max3,  self.milking_max3,
                    self.wet_header, self.milking_header
                    
                    ]


    def create_dataframes(self):
        
        self.milking = self.milking_dict
        
        m_head_1 = [item for sublist in self.milking_header[0] for item in sublist]
        m_head_2 = [item for sublist in self.milking_header[1] for item in sublist]
        m_head_3 = [item for sublist in self.milking_header[2] for item in sublist]
        m_head_4 = [item for sublist in self.milking_header[3] for item in sublist]
        m_head_5 = [item for sublist in self.milking_header[4] for item in sublist]
        m_head_6 = [item for sublist in self.milking_header[5] for item in sublist]

        l_head_1 = [item for sublist in self.wet_header[0] for item in sublist]
        l_head_2 = [item for sublist in self.wet_header[1] for item in sublist]
        l_head_3 = [item for sublist in self.wet_header[2] for item in sublist]
        l_head_4 = [item for sublist in self.wet_header[3] for item in sublist]
        l_head_5 = [item for sublist in self.wet_header[4] for item in sublist]
        l_head_6 = [item for sublist in self.wet_header[5] for item in sublist]
        
        
        self.milking = pd.DataFrame(self.milking,columns=m_head_1)

        self.milking_1 = pd.DataFrame(self.milking_dict[1][:,0,:],columns=m_head_1)
        self.milking_2 = pd.DataFrame(self.milking_dict[2][:,0,:],columns=m_head_2)
        self.milking_3 = pd.DataFrame(self.milking_dict[3][:,0,:],columns=m_head_3)
        self.milking_4 = pd.DataFrame(self.milking_dict[4][:,0,:],columns=m_head_4)
        self.milking_5 = pd.DataFrame(self.milking_dict[5][:,0,:],columns=m_head_5)
        self.milking_6 = pd.DataFrame(self.milking_dict[6][:,0,:],columns=m_head_6)

        self.lact_1 = pd.DataFrame(self.wet_dict[1][:,0,:],columns=l_head_1)
        self.lact_2 = pd.DataFrame(self.wet_dict[2][:,0,:],columns=l_head_2)
        self.lact_3 = pd.DataFrame(self.wet_dict[3][:,0,:],columns=l_head_3)
        self.lact_4 = pd.DataFrame(self.wet_dict[4][:,0,:],columns=l_head_4)
        self.lact_5 = pd.DataFrame(self.wet_dict[5][:,0,:],columns=l_head_5)
        self.lact_6 = pd.DataFrame(self.wet_dict[6][:,0,:],columns=l_head_6)

        return [
            self.lact_1, self.lact_2, self.lact_3,
            self.lact_4, self.lact_5, self.lact_6,
            self.milking_1, self.milking_2, self.milking_3,
            self.milking_4, self.milking_5, self.milking_6,
            self.milking
        ]
        
    def create_other_dfs(self):
        
        self.wet_days       = pd.DataFrame(self.wet_days3)
        self.wet_sum        = pd.DataFrame(self.wet_sum3)
        self.wet_mean       = pd.DataFrame(self.wet_mean3)
        self.wet_max        = pd.DataFrame(self.wet_max3)
        
        
        self.milking_days   = pd.DataFrame(self.milking_days3) 
        self.milking_sum    = pd.DataFrame(self.milking_sum3)
        self.milking_mean   = pd.DataFrame(self.milking_mean3)
        self.milking_max    = pd.DataFrame(self.milking_max3)
        
        return (self.wet_days, self.wet_sum, self.wet_mean, self.wet_max, 
                self.milking_sum, self.milking_days, self.milking_mean, self.milking_max
                )
    
                
    def create_write_to_csv(self):      
        self.wet_days            .to_csv('F:\\COWS\\data\\milk_data\\lactations\\wet_days.csv')
        self.wet_sum             .to_csv('F:\\COWS\\data\\milk_data\\lactations\\wet_sum.csv')
        self.wet_mean             .to_csv('F:\\COWS\\data\\milk_data\\lactations\\wet_mean.csv')        
        self.wet_max             .to_csv('F:\\COWS\\data\\milk_data\\lactations\\wet_max.csv')        
        
        self.milking_sum         .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_sum.csv')   
        self.milking_days        .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_days.csv')
        self.milking_mean        .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_mean.csv')            
        self.milking_max        .to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking_max.csv')         
        
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
        pass


if __name__ == '__main__':
    WetDry()