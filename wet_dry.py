import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as td

lb      = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'], header=0)
stop    = pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv',  parse_dates=['stop'], header=0)
bd      = pd.read_csv ('F:\\COWS\\data\\csv_files\\birth_death.csv', parse_dates=['birth_date','death_date'], header=0, index_col='WY_id')
start   = pd.read_csv ('F:\\COWS\\data\\csv_files\\live_births.csv', parse_dates=['b_date'],header=0)

milk   = pd.read_csv  ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', parse_dates=['datex'], header=0, index_col=0)

startpivot = start.pivot(index='WY_id', columns='calf#',    values='b_date')           #shape 127,6
stoppivot = stop.pivot  (index='WY_id', columns='lact_num', values='stop')
lbpivot = lb.pivot      (index='WY_id', columns='calf#',    values='b_date')

not_heifers1=lbpivot.index.values.tolist()  #bool mask to eliminate heifers - only contains cows that have calved

rng1    = [int(i) for i in list(milk.columns)]
rng     = pd.DataFrame(rng1, columns=['rng'])
rng.index += 1

#merge both start and stop with the blank array (rng) to make the columns of start and stop identical
start2 = startpivot.merge   (right=rng,how='right',left_index=True,right_index=True)   
stop2  = stoppivot.merge    (right=rng,how='right',left_index=True,right_index=True)

start2.index.name = 'WY_id'
start2.drop(['rng'], axis=1, inplace=True)
stop2.index.name  = 'WY_id'
stop2.drop(['rng'],axis = 1, inplace=True)

start1 = start2.T
stop1  = stop2.T

wy          = startpivot.index.tolist()            #list contains no heifers. . . .
last_stop   = stop1.max(axis=0)

diff_last_stop_deathdate = bd.death_date - last_stop        # integer days between last stop and death
diff_last_stop_deathdate = diff_last_stop_deathdate.dt.days

cowBdate = bd['birth_date']
cowDdate = bd['death_date']

# create array for milk
r  = len(milk)                            

headx   = '9/8/2018'
heady   = '9/1/2016'
head    = ((pd.to_datetime(headx, format='%m/%d/%Y')) - (pd.to_datetime(heady, format='%m/%d/%Y'))).days + 1
datex   =   pd.date_range (heady, headx, freq='d', name='datex')

milkfill  = np.zeros([head,len(milk.columns)])                #shape 708,201
milkfill1 = pd.DataFrame(milkfill,index=datex)

milkfill1.columns   = rng['rng'].apply(str)     #rng is an integer list of milk_cols (wy_ids)           
milkfill1.index     = pd.to_datetime(milkfill1.index)                  
milkx               = pd.concat((milkfill1 ,milk), axis=0)  #blank array from 9/1/2016 joined to 2018/8/10 to most recent milk day (len

# WET (completed lactation) + STILL MILKING
#
wet1, milking1 = 0, 0
wet2, wet3, milking2, milking3 = [], [], [], []
today =  pd.Timestamp.today()
#
rows = stop1.index            #integers
cols = rng1                    #integers    comes from line25 in section 1
#
for j in cols: 
    for i in rows:
        start = start1.   loc[i,j]
        stop = stop1.     loc[i,j]
        maxstart = start1.loc[:,j].max()
        maxstop = stop1.  loc[:,j].max()
        # dd = pd.to_datetime(bd['death_date'][j],format='%m/%d/%Y')
        dd = bd['death_date'][j]
#
        if( ( pd.isnull(start) == False)                  #if start and stop both exist 
            &   ( pd.isnull(stop) == False)
            &   ((dd >= maxstart) | pd.isnull(dd)==True)  # and cow is alive
            ):           
            wet1=(stop-start)/np.timedelta64(1,'D')       #this is a completed lactation              
#
        elif( (pd.isnull(start) == False)                 #if start exists ...
            &   (pd.isnull(stop)==True)                   #and stop does NOT, 
            &   (start==maxstart)                         #and this is the last start
            &   ((dd > maxstart) | pd.isnull(dd)==True)   #and cow is still alive
            ):
            milking1=(today - start)/np.timedelta64(1,'D')     #then it must be 'milking'
 #            
        elif( (pd.isnull(start) == False)                 #if start exists 
            &   (pd.isnull(stop)==True)                   #and stop does NOT, 
            &   (start==maxstart)                         #and this is the last lactation      
            &   (pd.isnull(dd) == False)                  #and the cow IS dead  
            ):
            wet1=(dd-start)/np.timedelta64(1,'D')         #then this is a completed lactation
 #              
        milking2.  append(milking1)
        wet2.      append(wet1)
        milking1, wet1=0,0
#
    wet3.append(wet2)
    milking3.append(milking2)
    wet2, milking2=[],[]
#   
milking4=pd.DataFrame(milking3)
milking4.index +=1
milking4.columns +=1
wet4=pd.DataFrame(wet3)
wet4.index +=1
#
z = []
j = 0
for j in cols:                                            #eliminates the 'None' from the lists
    x = milking4.T.loc[:,j]
    y = list(filter(None,x))
    z.append(y)
milking = pd.DataFrame(z,columns=['milking'])
milking.index += 1
#
wet4['milking']     = milking
wetsum              = wet4.sum(axis=1)
wet4['wetsum']      = wetsum
wet4_short          = wet4['wetsum']
wet4.rename(columns ={0:'w1', 1:'w2', 2:'w3', 3:'w4', 4:'w5', 5:'w6'},inplace=True)
wet4.index.name     = 'WY_id'

# WAITING :  this is different from dry because it is the maxstop - not between lactations
#
# waiting1 = 0
# waiting2,waiting3 = [], []
# #
# for j in cols:
#     for i in rows:
#         start       = start1.loc[i,j]
#         stop        = stop1. loc[i,j]
#         maxstart    = start1.loc[:,j].max()
#         maxstop     = stop1. loc[:,j].max()
# #  
#         if((        pd.isnull(start) == False)          #start exists
#             &   (   pd.isnull(stop ) == False)          #stop exists
#             &   (   pd.isnull(cowDdate[j]) == True)     #cow not dead yet
#             &   (   stop   == maxstop)                  #this stop IS the last one so far
#             &   (   start   == maxstart)                #means both start and stop are  their last values
#             &   (   maxstop > maxstart)                 #keeps out the 'still milking' cows
#             ): 
#             waiting1 =   (today - stop)/np.timedelta64(1,'D')                                                            
#             waiting2.append(waiting1) 
#             waiting1 = 0
# #
#     waiting3.append(waiting2)
#     waiting2 = []
# #   
# waiting4 = pd.DataFrame(waiting3)
# waiting4.index += 1
# #
# wait2 = []                 
# for j in cols:
#     wait1=[x for x in waiting4.T.loc[:,j]  if x !=[]]
#     wait2.append(wait1)
# waiting=pd.DataFrame(wait2)
# waiting.index +=1
# waiting.rename(columns = {0:'waiting'},inplace=True)

# DRY
#
dry1=0
dry2,dry3 = [],[]
for j in cols:
    for i in rows:              
# 
        maxstart=start1.loc[:,j].max()
        maxstop=stop1.loc[:,j].max()
        stopdryx=start1.shift(periods=-1,fill_value=None) 
        stopdry=stopdryx.loc[i,j]   
        startdry=stop1.loc[i,j]
#
        if(     ( pd.isnull(startdry) == False)                 #start exists
            &   ( stopdry <= maxstart)                          #for 'dry' the next start is the stop value
            ): 
            dry1=   (stopdry - startdry)/np.timedelta64(1,'D')  #so, dry is the stop-start diff
            dry2.append(dry1)                                                                          
        dry1=0
    dry3.append(dry2)
    dry2=[]
#
dry4=pd.DataFrame(dry3)
dry4.index +=1
dry4.columns +=1
dry4['last stop']=diff_last_stop_deathdate
# dry4['waiting']=waiting
drysum=dry4.sum(axis=1)
dry4['drysum']=drysum
dry4_short=dry4['drysum']
dry4.rename(columns={1:'d1',2:'d2',3:'d3',4:'d4',5:'d5'},inplace=True)
dry4.index.name='WY_id'

# MERGE WET AND DRY INTERVALS IN SEQUENCE
#
a3,b3,a2,b2={},[],[],[]
i=6
for j in rng1:  
    a=start1.loc[:i,j]
    b=stop1.loc[:i,j]
    a2.append(a)
    b2.append(b)
#
a3=pd.DataFrame(a2).add_suffix('w')
b3=pd.DataFrame(b2).add_suffix('d')
a4=a3[a3.index.isin(not_heifers1)]
b4=b3[b3.index.isin(not_heifers1)]
#
wetdrydatenames=    ['1w','1d','2w','2d','3w','3d','4w','4d','5w','5d','6w','6d']
wetdrydates=        pd.concat([a4,b4],       join='outer',axis=1,ignore_index=False).reindex(columns=wetdrydatenames)    
wetdryduration=     pd.concat([wet4,dry4],join='outer',axis=1,ignore_index=False)   #.reindex(columns=wetdrydatenames)   
wetdry_duration_short=  pd.concat([wet4_short,dry4_short],join='outer',axis=1,ignore_index=False)
wetdry_duration_short['wetdry_sum']=    wetdry_duration_short['wetsum'] + wetdry_duration_short['drysum']

# LIVE COWS AGE
#
alive1=                 bd['death_date'].isnull()
bd['tf']=               alive1                              #adds the bool mask to the df
alive=                  bd.loc[bd['tf']==True].copy()
age=(today - alive['birth_date'])/np.timedelta64(1,'D')     #age of all living cows
#
alive['age']=           age
calf1_bdate=            lbpivot[1]
alive['possible_days']= (today -calf1_bdate)/np.timedelta64(1,'D')
alive['ageAtCalf1']=    (calf1_bdate - alive['birth_date'])/np.timedelta64(1,'D')

# DEAD COWS AGE AT DEATH
#
dead1=bd['death_date'].notnull()
bd['tf']=dead1                                              #adds the bool (tf==truefalse) mask to the df
dead=bd.loc[bd['tf']==True].copy()
age=( dead['death_date'] - dead['birth_date'])/np.timedelta64(1,'D')
dead['age']=age
calf1_bdate=lbpivot[1]
dead['possible_days']=(dead['death_date'] -calf1_bdate)/np.timedelta64(1,'D')
dead['ageAtCalf1']=(calf1_bdate - dead['birth_date'])/np.timedelta64(1,'D')

# POSSIBLE DAYS
#
days=   pd.concat([alive,dead],axis=0,join='outer')
days    .drop(['dam_num','typex','tf'],axis=1,inplace=True)
days2=  days[days.index.isin(not_heifers1)].copy() 
days3=  days2.merge(right=wetdry_duration_short,how='outer',left_index=True,right_index=True,sort=True)
wetdry_all=days3
wetdry_all['comp']= wetdry_all['possible_days'] - wetdry_all['wetdry_sum']
wet_pct=            wet4['wetsum'] / days['possible_days']
dry_pct=            dry4['drysum'] / days['possible_days']
wetdry_all['wet_pct']=wet_pct
wetdry_all['dry_pct']=dry_pct

		
# wetdry        .to_csv('F:\\COWS\\data\\wetdry.csv')
#
wet4            .to_csv('F:\\COWS\\data\\wet_dry\\wet.csv')
dry4            .to_csv('F:\\COWS\\data\\wet_dry\\dry.csv')
wetdrydates     .to_csv('F:\\COWS\\data\\wet_dry\\wetdrydates.csv')   
wetdryduration  .to_csv('F:\\COWS\\data\\wet_dry\\wetdryduration.csv')   
wetdry_all      .to_csv('F:\\COWS\\data\\wet_dry\\wetdry_all.csv')
