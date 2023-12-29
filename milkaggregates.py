import pandas as pd
import numpy as np
import datetime 
import openpyxl
# import rawmilkupdate as rm
# import birthdeath as bd
# import insem_ultra as iu 

class MilkAggregates:
    def __init__(self):
        self.bd      = pd.read_csv       ('F:\\COWS\\data\\csv_files\\birth_death.csv', header=0, parse_dates=['birth_date', 'death_date'])
        self.all     = pd.read_csv       ('F:\\COWS\\data\\insem_data\\all.csv',        header=0)
        self.status  = pd.read_csv       ('F:\\COWS\\data\status\\status_col.csv',      header=0, index_col=0, )       

        self.lag     = -365
        print('lag = ', self.lag)
        
        self.basics()
        self.am, self.pm, self.fullday    = self.fullday_calc()
        self.tenday, self.tenday1         = self.ten_day()
        self.milk                         = self.create_avg_count()
        self.monthly, self.weekly, self. monthly_sum, self.monthly_mean         = self.create_monthly_weekly()
        self.write_to_csv()
    
    def basics(self):

        self.AM_liters = pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',   header=0, dtype=float)
        self.AM_wy   =   pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv',       index_col=0, header=0, dtype=float)
        self.PM_liters = pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv',   index_col=0, header=0)
        self.PM_wy   =   pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv',       index_col=0, header=0)
       
        self.wy      = self.bd['WY_id']
        self.alive1  = self.bd['death_date'].isnull()
        self.alive   = self.wy.loc[self.alive1].copy()
        self.alive.reset_index(drop=True,inplace=True)

        self.liters_am  = self.AM_liters .iloc[:, self.lag:]
        self.wy_am      = self.AM_wy     .iloc[:, self.lag:]
        self.liters_pm  = self.PM_liters .iloc[:, self.lag:]
        self.wy_pm      = self.PM_wy     .iloc[:, self.lag:]

        self.datex2      = self.liters_am.T.iloc[:,1:].copy()
        self.datex2.index.name = 'datex'
        self.date_format = '%m/%d/%Y'
        self.datex = pd.to_datetime(self.datex2.index, format='%m/%d/%Y') #datetime64[ns]

        self.maxcols     = len(self.datex)             #1575                          #len of dates (col headers for liters - which starts with 'start_date')
        self.maxrows     = len(self.bd['WY_id'])   #201                          #len of groupx - will accomodate new calves - continuous series from 1~200 will be the output col heading 

        self.idx     = np.zeros((self.maxrows+1,self.maxcols), dtype=int)    
        self.idx_am  = self.idx.copy()
        self.idx_pm  = self.idx.copy()

        # Numpyzation
        self.wy_am_np =    self.wy_am.      to_numpy(dtype=float)
        self.wy_pm_np =    self.wy_pm.      to_numpy(dtype=float)
        self.liters_am_np= self.liters_am.  to_numpy(dtype=float)
        self.liters_pm_np= self.liters_pm.  to_numpy(dtype=float)

    def fullday_calc(self):

        # static example -- don't erase
        # index1 = wy_am1[:,1]    #70,1869
        # index2 = index1[~np.isnan(index1)].astype(int)

        # value1 = liters_am1[:,1]   #70 1869
        # value2 = value1[~np.isnan(value1)].astype(float)
        # target1 = idx_am[:,1]      #243 1869
        # target1[index2] = value2

        #  AM calc
        idx_cols1   = [*range(1,self.maxcols)]         #list
        target_am = []
        i = 0

        while i < self.maxcols:
            index1 = self.wy_am_np[:,i]    #70,1869
            index2 = np.nan_to_num(index1, nan=0).astype(int)

            value1 = self.liters_am_np[:,i]   #70 1869
            value2 = np.nan_to_num(value1, nan=0.0).astype(float)
            target1 = self.idx_am[:,i].astype(float)      #243 1869

            target1[index2] = value2
            target_am.append(target1)
            i += 1
        am1 = pd.DataFrame(target_am)

        am2 = pd.DataFrame(am1)
        am = am2.T
        am.columns = self.datex
        am.replace(0,np.nan,inplace=True)
        am.drop(am.iloc[:,0:1],axis=1,inplace=True)

        

        #   PM calc
        idx_cols1   = [*range(1,self.maxcols)]         #list
        target_pm = []
        i = 0

        while i < self.maxcols:
            index1 = self.wy_pm_np[:,i]    #70,1869
            index2 = np.nan_to_num(index1, nan=0).astype(int)

            value1 = self.liters_pm_np[:,i]   #70 1869
            value2 = np.nan_to_num(value1, nan=0.0).astype(float)
            target1 = self.idx_pm[:,i].astype(float)      #243 1869

            target1[index2] = value2
            target_pm.append(target1)
            i += 1
        pm1 = pd.DataFrame(target_pm)


        pm2 = pd.DataFrame(pm1)
        pm = pm2.T
        pm.columns = self.datex
        pm.replace(0,np.nan,inplace=True)
        pm.drop(pm.iloc[:,0:1],axis=1,inplace=True)
        
        
        fullday1 = np.add(am1,pm1)  #cols are wy's, index is days
        fullday2 = pd.DataFrame(fullday1)
        fullday2['datex'] = self.datex
        fullday2.set_index('datex', inplace=True)

        fullday = fullday2
        fullday.index=pd.to_datetime(fullday.index).date
        fullday.replace(0,np.nan,inplace=True)

        fullday.drop(fullday.iloc[:,0:1],axis=1,inplace=True)
        fullday.index.name = 'datex'
        
        return am, pm, fullday
        
        
 

    def ten_day(self):

        lastday = self.fullday.iloc[-1:,:]     #last milking day recorded
        ld=lastday.loc[:,(lastday>0).any()].columns.tolist()     #the .any is important
        ld_nums = [int(x-1) for x in ld] #decrements the wy's
       
        tenday1 = self.fullday.iloc[-10:,:].copy() # has all wy's
        tenday2 = tenday1.loc[:,ld]           # has milkers only
        tenday3 = tenday1.iloc[:,ld_nums]      #unnecessary?
     
        tendayT=tenday2.T
    
        tendayT.columns=[1,2,3,4,5,6,7,8,9,10]
        tenday = tendayT
        avg = tenday.mean(axis=1).astype(float)
        tendayT['avg']=avg.round(1) 

        tenday.index.name='WY_id'
    
        # sumx = tenday.sum(axis=0).astype(float)
        # avgx = tenday.mean(axis=0).astype(float)
        # tenday.loc[''] = sumx.round(0)                   # [''] means 'empty row'
        tenday.loc['avg']   = tenday.mean(axis=0)
        tenday.loc['total'] = tenday.sum(axis=0)
        
        
        return tenday, tenday1
        
        

    def create_avg_count(self):
        # sum and nonzero count for entire milk df
        
        milk = self.fullday.copy()
        self.fullday.replace(np.nan,0,inplace=True)
        
        milkrowcount =   milk.astype(bool).sum(axis=1)
        milkrowsum =     milk.sum(axis=1,skipna=True)    #sum for that day, all cows
        milkcolsum =     milk.sum(axis=0,skipna=True)    #sum for that cow for all days
        
        milk['avg'] =    milkrowsum                      #blank col sets up the group agg
        milk['count'] =  milkrowcount
        milk.index =    self.datex
        milk.index.name = 'datex'
        milk.index =    pd.to_datetime(milk.index)
        
        return milk
        

    def create_monthly_weekly(self):

        self.milk['year']   = self.milk.index.year
        self.milk['month']  = self.milk.index.month
        self.milk['week']   = self.milk.index.isocalendar().week
        #  the as_index=False leaves the new columns accessible for .loc, otherwise they become part of a multi-index
        milk_monthly_sum    =   self.milk.groupby(['year','month'],          as_index=False).sum()    
        milk_monthly_mean1  =   self.milk.groupby(['year','month'],          as_index=False).mean()     
        weekly              =   self.milk.groupby(['year','month','week'],   as_index=False).mean() 

        # change names because 'sum' will eventually mean the monthly total vs the avg
        milk_monthly_mean1.rename(columns={'avg':'avg sum','count':'avg count'},inplace=True)
        # cuts out the middle cols
        monthly1       = milk_monthly_mean1.iloc[-12:,[0,1,-2,-3]].copy()
    
        monthly1['total'] = milk_monthly_sum['avg']

        def format_num(num):
            return '{:,.0f}'.format(num)

        monthly1[['avg count', 'avg sum', 'total']] = monthly1[['avg count', 'avg sum', 'total']].map(format_num)
        monthly = monthly1.reset_index(drop=True)
        monthly_sum = milk_monthly_sum
        monthly_mean = milk_monthly_mean1
        
        return monthly, weekly, monthly_sum, monthly_mean



    def write_to_csv(self):

        self.am.to_csv('F:\\COWS\data\\milk_data\\halfday\\am\\am.csv')  #these are useful to check against daily_milk to see if the data is aligned
        self.pm.to_csv('F:\\COWS\data\\milk_data\\halfday\\pm\\pm.csv')
        # 
        fullday_lastdate = pd.DataFrame(index=[self.fullday.index[-1]], columns=['last_date'])

        # 
        self.fullday.        to_csv('F:\\COWS\data\\milk_data\\fullday\\fullday.csv')
        fullday_lastdate.to_csv('F:\\COWS\data\\milk_data\\fullday\\fullday_lastdate.csv')
        # 
        self.weekly.         to_csv('F:\\COWS\data\\milk_data\\totals\\weekly.csv')
        self.monthly.        to_csv('F:\\COWS\data\\milk_data\\totals\\monthly.csv')
        self.monthly_sum.        to_csv('F:\\COWS\data\\milk_data\\totals\\monthly_sum.csv')
        self.monthly_mean.        to_csv('F:\\COWS\data\\milk_data\\totals\\monthly_mean.csv')
        
        #
        self.milk.           to_csv('F:\\COWS\data\\milk_data\\fullday\\milk.csv')
        self.tenday.         to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.csv')
        self.tenday1.        to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday1.csv')
        # self.all2.           to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\ten day2.csv')
        # 
        with pd.ExcelWriter('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\output.xlsx') as writer:
            self.tenday.      to_excel(writer, sheet_name='tenday')
            self.tenday1.     to_excel(writer, sheet_name='tenday2')

        # 
        print(self.tenday.iloc[:1,:])
