import pandas as pd
import numpy as np
import datetime as dt
from datetime import date

lb=pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv')
bd=pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv')
u=pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv')
i=pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv')
s=pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv')
f=pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',index_col=0)

lb.set_index('WY_id',inplace=True)
bd.set_index('WY_id',inplace=True)
u.set_index ('WY_id',inplace=True)
i.set_index ('WY_id',inplace=True)
s.set_index ('WY_id',inplace=True)

lb['b_date']=    pd.to_datetime(lb['b_date'],infer_datetime_format=True)
bd['death_date']=pd.to_datetime(bd['death_date'],infer_datetime_format=True)
bd['birth_date']=pd.to_datetime(bd['birth_date'],infer_datetime_format=True)
u['ultra_date']= pd.to_datetime(u['ultra_date'],infer_datetime_format=True)
i['insem_date']= pd.to_datetime(i['insem_date'],infer_datetime_format=True)
s['stop']=       pd.to_datetime(s['stop'],infer_datetime_format=True)

#count of milking cows on last milking day on record

lmd=f.T.iloc[:,-1] .copy()
lmd_count=lmd.count() 
status['milking']=lmd_count
# list of WYs actually milking on last day
lmd.fillna(0,inplace=True)
lmd3=lmd.to_numpy().nonzero()
lmd4=[x+1 for x in lmd3]
lmd5=pd.DataFrame(lmd4)
lmd6=lmd5.T
lmd6.set_index([0],drop=True,inplace=True)
lmd6['m']='m'
lmd6.index.name='WY_id'
status_milking=status1.merge(lmd6,how='left',on='WY_id')
# need to run through this loop to get a regular list to join (below) with the heifer list
lmd2=[]
for i in lmd.index:
    if lmd[i]>0:
        lmd2.append(i)
    elif lmd[i]==0:
        pass

lmd2int=[int(i) for i in lmd2]
heiferlistint=[int(i) for i in heiferlist]  
# dry = alive - (heifers + milking)
# dry1=(set(lmd2) + set(heiferlist))
# dry=list(set(alivelist) -set(dry1))
# dry1=list(set(alivelist) -set(dry1))

notdry=[*lmd2 , *heiferlist]
dry=set(notdry).difference(set(alivelist))
len(df),'',len(alivelist),'',len(deadlist),'',len(notdry),'',len(lmd2),'',len(heiferlist),'',len(dry) #(224, '', 100, '', 124, '', 80, '', 22, '', 58)
alive=[*dry,*lmd2,*heiferlist]
len(alive) 


#  stop last

stop_last=s.groupby("WY_id").agg({
    'stop':'max',                       
    'lact_num':'max'
}).copy()
stop_last.rename(columns={'stop':'stop_last','lact_num':'sl_calf#'},inplace=True)
sl=df.join(stop_last)

#  FIRST AND LAST CALF

lb_last=lb.groupby('WY_id').agg({           #returns a df
    'calf#':'max',
    'b_date':'max'}).copy()
# lb_last.reset_index(inplace=True)
#
lb_first=lb.groupby('WY_id').agg({           #returns a df
    'calf#':'min',
    'b_date':'min'}).copy()
# lb_first.reset_index(inplace =True)
#
lb_last.rename(columns={'b_date':'lastcalf bdate','calf#':'last calf#'},inplace=True)
lb_first.rename(columns={'b_date':'1st_bdate'},inplace=True)
lb3=df.join(lb_last)      #sl=stop_last and is indexed 1-200ish


#  merge LAST ULTRA WITH reading
#
u1a=u.groupby('WY_id',as_index=True).agg({
    'Calf_num':'max',
    'ultra_date' :'max'   }).copy()
mcols=['WY_id','Calf_num','ultra_date']
u1b=lb3.merge(u1a,on=('WY_id'),suffixes=('lb','u'))
u1c=u1b.loc[  (u1b['Calf_num'] > u1b['last calf#'])   ]
u1=u1c[['Calf_num','ultra_date']]
#
u2=u1.merge (right=u,how='left',left_on=mcols,right_on=mcols)
u2.rename   (columns={'Calf_num':'u_calf#','ultra_date':'u_date','ultrareading':'readex'},inplace=True)
# 
u3a=u2[['u_calf#','u_date','readex']]
u3=df.join(u3a)  #sl=stop_last and is indexed 1-200ish


#  LAST INSEM DATE  

i1a=i.groupby('WY_id',as_index=True).agg({
    'calf_num':'max',
    'insem_date':'max'
}).copy()
i1a.rename(columns={'calf_num':'i_calf#','insem_date':'i_date'},inplace=True)
Lcols=  ["WY_id",'i_calf#','i_date']
Rcols= ["WY_id",'calf_num','insem_date']
#
i1b=lb3.merge(i1a,on='WY_id')
i1c=i1b.loc[       (i1b['i_calf#'] > i1b['last calf#'])   ]  #last calf# is from livebirths
i1=i1c[['i_calf#','i_date']]
i2=i1.merge(right=i,how='left',left_on=Lcols,right_on=Rcols)
i3a=i2[['i_calf#','i_date']]
i3=df.join(i3a)

#  merge last ultra with last insem date and last b_date and last stop

x=u3.   merge(right=lb3, how='outer',on='WY_id')
x2a=x.  merge(right=i3, how='outer',on="WY_id")
x2=x2a. merge(right=bd[['birth_date','death_date','dam_num']],how='outer',on='WY_id')
x2.     rename(columns={'dam_num':'dam'},inplace=True)
#
next_bdate=   x2['i_date'] + pd.DateOffset(days=272)
next_stopdate=x2['i_date'] + pd.DateOffset(days=637)
x2['next_bdate']= next_bdate
x2['stop_next']=  next_stopdate
#
x3=x2.join(sl)     #sl= stop_last
x3.rename(columns={'last_bdate':'lastcalf bdate','next_bdate':'nextcalf bdate','birth_date':'cow bdate'},inplace=True)
all=x3
allidx=pd.Series(all.index.values.tolist())


# create df of birthdates for expected births in future

all1=all.loc[all['dam'].notnull()]
nb=all1.loc [                                   #nb = 'new births'
        (all['death_date'].isnull())
    &   (all['readex'] == 'ok')
    &   ( (all['i_calf#'] > all['last calf#']))
    ].copy()


nextcalf_bdate=nb['i_date']+pd.DateOffset(days=272)
nb['cow bdate']=nextcalf_bdate
# nb['next stopdate'] =nb['i_date']+pd.DateOffset(days=(272+308))
nb.sort_values(by='cow bdate',inplace=True)
nb.reset_index(drop=False,inplace=True)
nb.drop(['dam'],axis=1,inplace=True)
# nb.reset_index(drop=False,inplace=True)
nb.rename(columns={'WY_id' : 'dam'},inplace=True)


start=  int     (bd.index.max()+1)
end=    int     (len(nb)+start)
idx1=   np.arange(start,end,1,dtype=int)
idx=    pd.Series(idx1,name='WY_id')
nb['WY_id']=idx
nb.set_index    ('WY_id',drop=True,inplace=True)
nb2=nb.loc[:,('dam','cow bdate')].copy()
#
all2=pd.concat([all1,nb2])             
all2.fillna('',inplace=True)


# rearrange columns
               
x4=x3.      iloc[:,[8,7,9,4,11,10,12,3,5,0,6,1,2,8]].copy()           
all3=all2.  iloc[:,[9,8,7,12,4,10,11,3,5,0,6,1,2]]

# calc age of last calf = wks milking

lcb=            all3['lastcalf bdate']
tdy=            pd.to_datetime("today")
wks_milk=       (tdy -lcb) / np.timedelta64(1,'W')
wks_milk.name=  'wks_milk'
all3['wks_milk']=wks_milk

lb_first.to_csv ('F:\\COWS\\data\\insem_data\\lb_first.csv')
lb_last.to_csv  ('F:\\COWS\\data\\insem_data\\lb_last.csv')
u3.to_csv       ('F:\\COWS\\data\\insem_data\\ultra_last_all.csv')
all.to_csv      ('F:\\COWS\\data\\insem_data\\last_live_ultra_insem_merge.csv')
nb2.to_csv      ('F:\\COWS\\data\\insem_data\\expected_cows.csv')
all3.to_csv     ('F:\\COWS\\data\\insem_data\\status.csv')