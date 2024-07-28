'''wet_dry.py'''

import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as td

lb      = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'], header=0)
stop    = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  parse_dates=['stop'], header=0)
bd      = pd.read_csv ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], header=0, index_col='WY_id')
start   = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'],header=0)

milk    = pd.read_csv  ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', parse_dates=['datex'], header=0, index_col=0)


class WetDry:
    def __init__(self):

        self.startpivot  = start.pivot (index='WY_id', columns='calf#',    values='b_date').reset_index()          
        self.stoppivot   = stop .pivot (index='WY_id', columns='lact_num', values='stop').reset_index()
        self.lbpivot     = lb   .pivot (index='WY_id', columns='calf#',    values='b_date').reset_index()

        self.startpivot .set_index('WY_id', inplace=True)
        self.stoppivot  .set_index('WY_id', inplace=True)
        self.lbpivot    .set_index('WY_id', inplace=True)

        self.lb         = lb
        self.stop       = stop
        self.bd         = bd
        self.start          = start
        self.milk           = milk
        self.milk_cols      = milk.columns.to_frame()

        self.calf1_bdate    = self.lbpivot[1]



      #functions
        (self.today,    self.not_heifers1, 
        self.start_1,   self.stop_1, 
        self.start_2,   self.stop_2,         
        self.cowBdate,  self.cowDdate,
        self.max_start, self.max_stop,
        self.diff_last_stop_deathdate)              = self.create_basics()
        
        self.milk_array                             = self.create_array_for_milk()

        ( self.cols,    self.rows, 
        self.wet3,      self.wet,       self.wetsum,        
        self.milking3,  self.milking,   self.milkingmax
        )                                           = self.create_wet()
        
        
        self.dry, self.drysum                       = self.create_dry()

        (self.stop_death_gap)                       = self.create_days_laststop_death()

        (self.wetdry_dates, self.wetdry_duration,
        self.wetdry_sum)                            = self.merge_wet_dry_intervals_in_sequence()
    
        self.bd_alive, self.alive_mask              = self.create_live_cows_age()
        
        self.bd_dead,  self.death_mask              = self.create_dead_cows_age()
        
        self.wetdry_all                             = self.create_wetdry_all()


    def create_basics(self):

        today =  pd.Timestamp.today()
        not_heifers1 = self.lbpivot.index.values.tolist()  #eliminate heifers - only contains cows that have calved
        
        #reindex both start and stop with the WY_id's from the milk df
        start_2 = self.startpivot.reindex(self.milk.T.index.astype(np.int64))
        stop_2  = self.stoppivot .reindex(self.milk.T.index.astype(np.int64))

        start_1 = start_2.T
        stop_1  = stop_2.T
        
        start_1.rename_axis( 'WY_id', inplace=True)
        stop_1 .rename_axis( 'WY_id', inplace=True)

        cowBdate = bd['birth_date']
        cowDdate = bd['death_date']

        max_start1 = start_2.max(axis=1)
        max_stop1  = stop_2 .max(axis=1)
        max_start  = max_start1.reindex(self.milk.T.index.astype(np.int64))
        max_stop   = max_stop1.reindex(self.milk.T.index.astype(np.int64))

        diff_last_stop_deathdate1 = bd.death_date - max_stop        # integer days between last stop and death
        diff_last_stop_deathdate = diff_last_stop_deathdate1.dt.days

        return today, not_heifers1, \
            start_1, stop_1,\
            start_2, stop_2,\
            cowBdate, cowDdate,\
            max_start, max_stop,   diff_last_stop_deathdate


    def create_array_for_milk(self):
        r  = len(self.milk)                            

        headx   = '8/9/2018'
        heady   = '9/1/2016'
        head    = ((pd.to_datetime(headx, format='%m/%d/%Y')) - (pd.to_datetime(heady, format='%m/%d/%Y'))).days + 1
        # head is number of days betwee headx and heady
        
        datex   =   pd.date_range (heady, headx, freq='d', name='datex')

        milkfill  = np.zeros([head,len(self.milk.columns)])
        milkfill1 = pd.DataFrame(milkfill,index=datex)

        milkfill1.columns   = self.milk.columns
        milkfill1.index     = pd.to_datetime(milkfill1.index)                  
        milk_array          = pd.concat((milkfill1 ,milk), axis=0).sort_index()  #blank array from 9/1/2016 joined to 2018/8/10 to most recent milk day (len
        milk_array.replace(0, "", inplace=True )

        return milk_array
    


    def create_wet(self):           # WET (completed lactation) + STILL MILKING

        (   wet2,         wet3, 
            milking2,     milking3,     
        )    =   [], [], [], []
        
        rows = list( self.stop_1.index)      #integers
        cols = self.start_1.columns  #integers decremented by 1
        
        for j in cols:  #iterates first down the rows in 'start' and 'stop' then across the cols- the wy's
            for i in rows:
                start       = self.start_1          .loc[i,j]
                stop        = self.stop_1           .loc[i,j]
                max_start_x = self.max_start [j]
                max_stop_x  = self.max_stop  [j]
                dd          = bd['death_date'][j]
               
        
                if( ( pd.isnull(start) == False)                  #if start and stop both exist 
                    &   ( pd.isnull(stop) == False)
                    &   ((dd >= max_start_x) | pd.isnull(dd)==True)  # and cow is bd_alive
                    ):           
                    wet1=(stop-start)/np.timedelta64(1,'D')       #this is a completed lactation

                    milking1 = ''
          
        
                elif( (pd.isnull(start) == False)                 #if start exists ...
                    &   (pd.isnull(stop)==True)                   #and stop does NOT, 
                    &   (start==max_start_x)                         #and this is the last start
                    &   (((dd > max_start_x) | pd.isnull(dd)==True))
                    ):

                    milking1=(self.today - start)/np.timedelta64(1,'D')     #then it must still be 'milking'


                elif( (pd.isnull(start) == True)                 #if neither exists then
                    &   (pd.isnull(stop)==True)                   # fill with a blank
                    &   (start==max_start_x)                       
                ):
                  milking1, wet1  = "",""
                  
                wet2.append(wet1)

                
                if 'milking1' in locals():  # use this if the var might not exist eg 'milking'
                    milking2.append(milking1)

                (wet1, milking1) =    '', ''

            wet3    .append(wet2)
            milking3.append(milking2)

            wet2, milking2 = [],[]

            wetnp       = np.array(wet3)
            milkingnp   = np.array(milking3)



        wet        = pd.DataFrame(wetnp, columns=rows)
        wet.replace('', np.nan, inplace=True)
        wet = wet.astype(float) 

        milking    = pd.DataFrame(milkingnp, columns=rows) 
        
        wetsum     = wet.sum(axis=1)
        milkingmax = milking.max(axis=1)

        wet['wet sum']   = wetsum
        wet['milking']  = milkingmax

        wet     .index += 1
        wetsum  .index += 1
        milking .index += 1
    

        return (cols,           rows,     
                wet3,           wet,        wetsum,        
                milking3,       milking,    milkingmax
                )
    x=1
    


    def create_dry(self):

        dry1=0
        dry2,dry3 = [],[]
        for j in self.cols:
            for i in self.rows:              
         
                max_start    = self.max_start[j]
                max_stop     = self.max_stop[j]

                start_x     = self.start_1.shift(-1)

                startdry    = self.stop_1.loc[i,j]  
                stopdry     = start_x.loc[i,j]  # the end of the dry is the start of the next lactation
                max_start_x = pd.to_datetime(max_start)
        
                if(     ( pd.isnull(startdry) == False)   #start exists
                    and   ( stopdry <= max_start_x)            #for 'dry' the next start is the stop value
                    ): 
                    dry1=   (stopdry - startdry)/np.timedelta64(1,'D')  #so, dry is the stop-start diff
                    dry2.append(dry1)                                                                          
                dry1=0
            dry3.append(dry2)
            dry2=[]
        
        dry4 = pd.DataFrame(dry3)
        dry4.index +=1
        dry4.columns +=1
        dry4['last stop']   = self.diff_last_stop_deathdate

        drysum=dry4.sum(axis=1)
        dry4['drysum']      = drysum
        dry4_short=dry4['drysum']
        dry4.rename(columns = {1:'d1',2:'d2',3:'d3',4:'d4',5:'d5'},inplace=True)
        dry4.index.name     = 'WY_id'

        dry = dry4
        return dry, drysum
    
    def create_days_laststop_death(self):

       

        filldate =  pd.to_datetime('1900-01-01')
        adj_dd = self.bd['death_date'].fillna(filldate).to_frame()

        adj_dd.reset_index(drop=True, inplace=True)
        adj_max_stop = self.max_stop.fillna(filldate).shift(-1)

        m1 = pd.merge(adj_dd, adj_max_stop.to_frame(), how='left', left_index=True, right_index=True)
        stop_death_gap1 = m1
        gap = (m1['death_date'] - m1[0]).dt.days

        stop_death_gap1['gap'] = gap
        stop_death_gap2 = stop_death_gap1.mask(stop_death_gap1['gap'] <0, np.nan)
        stop_death_gap2.index += 1

        stop_death_gap = stop_death_gap2.reindex(self.bd.index)
        stop_death_gap.rename(columns={1 : 'max stop'}, inplace=True)

        return stop_death_gap


    def merge_wet_dry_intervals_in_sequence(self):
        a3,b3,a2,b2=[],[],[],[]

        cols = self.start_1.columns
        i=6
        for j in cols:  
            a = self.start_1.loc[:i,j]
            b = self.stop_1.loc[:i,j]
            a2.append(a)
            b2.append(b)
        #
        a3 = pd.DataFrame(a2).add_suffix('w')
        b3 = pd.DataFrame(b2).add_suffix('d')
        a4 = a3[a3.index.isin(self.not_heifers1)]
        b4 = b3[b3.index.isin(self.not_heifers1)]
        #
        wetdry_datenames =    ['1w','1d','2w','2d','3w','3d','4w','4d','5w','5d','6w','6d']
        wetdry_dates =        pd.concat([a4,b4], 
                                        join ='outer',
                                        axis=1,
                                        ignore_index=False).reindex(columns=wetdry_datenames)  
          
        wetdry_duration =     pd.concat([self.wet, self.dry],
                                        join='outer',
                                        axis=1,
                                        ignore_index=False)

        wetdry_sum =   pd.concat([self.wetsum, self.drysum], 
                                        join='outer', 
                                        axis=1,  
                                        ignore_index=False)
        
        wetdry_sum['total days'] = (self.wetsum + self.drysum)



        return wetdry_dates, wetdry_duration, wetdry_sum

    def create_live_cows_age(self):
        
        alive_mask   = bd['death_date'].isnull()           
        bd_alive    = bd.loc[alive_mask].copy()

        age_alive = (self.today - bd_alive['birth_date'])/np.timedelta64(1,'D')     #age of all living cows
        bd_alive['age']             = age_alive

        bd_alive['ageAtCalf1']      = (self.calf1_bdate - bd_alive['birth_date'])/np.timedelta64(1,'D')
        bd_alive['possible_days']   = (self.today - self.calf1_bdate)/np.timedelta64(1,'D')

        return bd_alive, alive_mask


    def create_dead_cows_age(self):
    
        death_mask       = bd['death_date'].notnull()    #one col T-F mask
        bd_dead        = bd.loc[death_mask].copy()
        age_dead         = ( bd_dead['death_date'] - bd_dead['birth_date'])/np.timedelta64(1,'D')
        bd_dead['age'] = age_dead

        bd_dead['ageAtCalf1']       = (self.calf1_bdate      - bd_dead['birth_date'])/np.timedelta64(1,'D')
        bd_dead['possible_days']    = (bd_dead['death_date'] - self.calf1_bdate)/np.timedelta64(1,'D')
        
        return  bd_dead, death_mask

    def create_wetdry_all(self):
        #
        days1=   pd.concat([self.bd_alive, self.bd_dead], axis=0, join='outer').sort_values('WY_id')

        field_names = ['birth_date','dam_num','typex','arrived','readex']
        days1    .drop(field_names, axis=1, inplace=True)
        wetdry_all =  days1.loc[self.not_heifers1].copy() 


        wetdry_all['wetdry_sum'] =self.wetdry_sum['total days']
        wetdry_all['wet']       = self.wetsum
        wetdry_all['dry']       = self.drysum
        wetdry_all['milking']   = self.milkingmax
        wetdry_all['gap']       = self.stop_death_gap['gap']



        wetdry_all['comp'] = wetdry_all['possible_days'] - wetdry_all['wetdry_sum']
        wet_pct1=            self.wetsum / wetdry_all['possible_days']
        dry_pct1=            self.drysum / wetdry_all['possible_days']
        
        wetdry_all['wet_pct'] = wet_pct1
        wetdry_all['dry_pct'] = dry_pct1

        return wetdry_all

                
    def create_write_to_csv(self):      
        self.wet             .to_csv('F:\\COWS\\data\\wet_dry\\wet.csv')
        self.dry             .to_csv('F:\\COWS\\data\\wet_dry\\dry.csv')
        self.wetdry_dates    .to_csv('F:\\COWS\\data\\wet_dry\\wetdrydates.csv')   
        self.wetdry_duration .to_csv('F:\\COWS\\data\\wet_dry\\wetdryduration.csv')   
        self.wetdry_all      .to_csv('F:\\COWS\\data\\wet_dry\\wetdry_all.csv')







