import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, timedelta
#
lb=pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv',parse_dates= ['b_date'])
bd=pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',parse_dates=['birth_date','death_date'])
u=pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv',      parse_dates= ['ultra_date'])
i=pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv',      parse_dates= ['insem_date']) 
s=pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv', parse_dates= ['stop'])
f=pd.read_csv ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',index_col=0,parse_dates= ['datex'])
sc=pd.read_csv ('F:\\COWS\\data\\status\\status_column.csv',index_col=0)
#
lb.set_index('WY_id',inplace=True)
bd.set_index('WY_id',inplace=True)
u.set_index ('WY_id',inplace=True)
i.set_index ('WY_id',inplace=True)
s.set_index ('WY_id',inplace=True)
#
rng=list(range(1,bd.index.max()+1))
df=pd.DataFrame()
df['WY_id']=rng
df.set_index('WY_id',drop=True,inplace=True)
#
alivemask=bd.loc[bd['death_date'].isnull()].index.tolist()
today=np.datetime64('today','D')

# Last calf, last stop, first calf
#
stop_last=s.groupby("WY_id").agg({
    'stop':'max',                       
    'lact_num':'max'
}).copy()
stop_last.rename(columns={'stop':'stop_last','lact_num':'sl_calf#'},inplace=True)
sl=df.join(stop_last)
sl['sl_calf#'].fillna(0,inplace=True)
#
#  First and last calf
#
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
lb3['last calf#'].fillna(0,inplace=True)

#
# Last insem
#
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
#
i4=lb_last.merge(i3,how='right',left_index=True,right_index=True)
i4['last calf#'].fillna(0,inplace=True)
i5=i4.loc[
    ((    i4['i_calf#'] > i4['last calf#'])  )
]
i6=i5[['i_calf#','i_date']]

# Last ultra
#
u1a=u.groupby('WY_id',as_index=True).agg({
    'Calf_num':'max',
    'ultra_date' :'max'   }).copy()
mcols=['WY_id','Calf_num','ultra_date']
u1b=lb3.merge(u1a,on=('WY_id'),suffixes=('lb','u'))
u1c=u1b.loc[
          ((u1b['Calf_num'] > u1b['last calf#'])  |  (u1b['last calf#'].isnull()) )
          ]
#
u1=u1c[['Calf_num','ultra_date']]
#
u2=u1.merge (right=u,how='left',left_on=mcols,right_on=mcols)
u2.rename   (columns={'Calf_num':'u_calf#','ultra_date':'u_date','ultrareading':'readex'},inplace=True)
# 
u3a=u2[['u_calf#','u_date','readex']]
u3=df.join(u3a)  
#  
u4=lb_last.merge(u3,how='right',left_index=True,right_index=True)
u5=u4.loc[
    ((    u4['u_calf#'] > u4['last calf#']) |  (u4['last calf#'].isnull()))
    ].copy()
u6=u5[   [ 'u_calf#','u_date','readex']].copy()


# Merge ultra, insem, last bdate, last stop
#
x  = bd.merge(right=lb3,on='WY_id')
x1 = x.loc[alivemask].copy()                                   # filter all live 
#
cows =        x1.loc[x1['lastcalf bdate'].notnull()]     #filter out the heifers
heifers1 =   x1.loc[x1['lastcalf bdate'].isnull()]
#
x2= cows.merge(right=i6,how='left',on='WY_id')
x3= x2.merge(right=u6,how='left',on='WY_id')
x4= x3.merge(right=sl,how='left',on='WY_id')
x4.rename(columns={'dam_num':'dam'},inplace=True)
# all1.drop(['death_date','typex'],axis=1,inplace=True)

#
# valid insem
x_insem = x4.loc[ (x4['i_calf#'] > x4['last calf#'])   ]
x_insem_list    = list(x_insem.index)
#
x_ultra = x4.loc[ ((x4['u_calf#'] > x4['last calf#'])  
    &  (x4['u_date'] > x4['stop_last'])
    &  (x4['u_date'] > x4['i_date'])
    ) ]
x_ultra_list    = list(x_ultra.index)
#
x_readex_list = x_ultra_list
#
x_insem2 = x4.loc[x_insem_list]
x_ultra2 = x4.loc[x_ultra_list]
x_readex2 = x4.loc[x_readex_list]
#
x_insem3 = x_insem2['i_date']
x_ultra3 = x_ultra2['u_date']
x_readex3 = x_readex2['readex']

#
# filter
#
x5a = x4.merge(right=x_insem3,how='left',on='WY_id',suffixes=('_remove',""))
x5a.drop([i for i in x5a.columns if 'remove' in i],axis=1,inplace=True)
#
x5b = x5a.merge(right=x_ultra3,how='left',on='WY_id',suffixes=('_remove',""))
x5b.drop([i for i in x5b.columns if 'remove' in i],axis=1,inplace=True)
#
x5c = x5b.merge(right=x_readex3,how='left',on='WY_id',suffixes=('_remove',""))
x5c.drop([i for i in x5c.columns if 'remove' in i],axis=1,inplace=True)
#
all1 = x5c
all1.drop(['death_date','typex'],axis=1,inplace=True)
all1.rename(columns={'birth_date':'cow bdate'},inplace=True)
#
nextcalf_bdate1=all1.loc[
    ( all1['i_date' ].notnull()
    & ( all1['readex'] == 'ok'))
    ]
nextcalf_bdate=nextcalf_bdate1['i_date'] +  timedelta(days= 282)
all1['next bdate']=nextcalf_bdate


# age
#  
all2 = all1
#
all2['age cow']             =((today - all2['cow bdate'])/np.timedelta64(1,'M'))
all2['age lastcalf bdate']  =(today - all2['lastcalf bdate']).dt.days
all2['age last insem']      =(today - all2['i_date']).dt.days
all2['age last ultra']      =(today - all2['u_date']).dt.days
#
all2['days left']           =-(today - all2['next bdate'])/np.timedelta64(1,'D')
all2['days waiting']         =(all2['lastcalf bdate'] - all2['stop_last'])/np.timedelta64(1,'D')
#
all2['insem-ultra']         =all2['age last insem'] - all2['age last ultra']
all2["ultra(e)"]            =all2['i_date'] + timedelta(days= 40)
#
all2['insem-ultra']         =all2['age last insem'] - all2['age last ultra']
all2["ultra(e)"]            =all2['i_date'] + timedelta(days= 40)
#
all2intlist=['insem-ultra' 
,'age last ultra'
,'u_calf#'
,'last calf#'
,'i_calf#'
,'sl_calf#'
,'age cow'
]
all2['status']=sc['status']

# re-order columns
#
all=all2.loc[:,[
'status','age cow',#'death_date',
'stop_last','lastcalf bdate','days waiting',
'sl_calf#','last calf#','i_calf#','u_calf#',
'i_date','u_date','readex',
'next bdate',
'age lastcalf bdate','age last insem','age last ultra',
'insem-ultra','ultra(e)','days left'
]].copy()
#
all_list=list(all.index)

# NEW COLUMNS

# Pregnant
#
preg2=all.loc[
    (all['readex']=='ok')
    & (all['i_calf#']   > all['last calf#'])
    & ((all['i_date']    > all['lastcalf bdate'] ) | (all['lastcalf bdate'].isnull()))
    ].copy()
preglist=list(preg2.index)
preg=preg2.loc[:,[
# 'dam','cow bdate','death_date',
'status','stop_last','lastcalf bdate','readex',
# 'sl_calf#','last calf#','i_calf#','u_calf#',
'i_date','u_date',
# 'stop_next',
'next bdate',
# 'age cow','age lastcalf bdate','age last insem','age last ultra',
'insem-ultra','days left'
# ,'ultra(e)'
]]
pregcols=['stop_next']
preg=preg.sort_values('next bdate')

# Not pregnant
#
notpreglist=list(set(all_list).difference(set(preglist)))
# notpregsym=set(preglist.symmetric_difference(all_list))
notpreg2=all.loc[notpreglist]
notpreg=notpreg2.loc[:,[
# 'dam','cow bdate','death_date',
'status','stop_last','lastcalf bdate',
'sl_calf#','last calf#','i_calf#','u_calf#','readex',
'i_date','u_date','readex',
# 'stop_next',
'next bdate',
# 'age cow',
'age lastcalf bdate','age last insem','age last ultra',
'insem-ultra'
# ,'days left'
# ,'ultra(e)'
]].copy()
notpreg.sort_index(inplace=True)

# too early (no heifers)
#
tooEarly2=notpreg.loc[
    (notpreg['age last insem']<40) & (notpreg['status']=='M')
    ].copy()
tooearlylist_milking=list(tooEarly2.index)
tooEarly=tooEarly2.loc[:,[
# 'dam','cow bdate','death_date',
'status','stop_last','lastcalf bdate',
'sl_calf#','last calf#','i_calf#','u_calf#',
'i_date',
# 'u_date','readex',
# 'stop_next',
# 'next bdate',
# 'age cow',
'age lastcalf bdate',
'age last insem',
'age last ultra',
# 'insem-ultra'
# ,'days left'
# ,'ultra(e)'
]]
#


# no insem  (no heifers)
#
no_insem2=notpreg.loc[
    (notpreg['age lastcalf bdate']>=40 )
    & (notpreg['i_date'].isnull())
    ].copy()
no_insem2_list=list(no_insem2.index)
no_insem2=no_insem2.sort_values('age lastcalf bdate')
noinsemcol=['sl_calf#','age lastcalf bdate','last calf#']
no_insem2[noinsemcol]=no_insem2[noinsemcol].astype('int')
no_insem=no_insem2.loc[:,[
# 'dam','cow bdate','death_date',
'status','stop_last','lastcalf bdate',
'sl_calf#','last calf#',
# ,'i_calf#','u_calf#','i_date',
# 'u_date','readex',
# 'stop_next',
# 'next bdate',
# 'age cow',
'age lastcalf bdate',
# 'age last insem','age last ultra',
# 'insem-ultra'
# ,'days left'
# ,'ultra(e)'
]].copy()

# Ultra out of date
#
uod2=notpreg[(notpreg['insem-ultra'] < 0)
    & (notpreg['age last insem']>=40)
]
uod_list=list(uod2.index)
uod=notpreg.loc[uod_list]


# all heifers
#
heifers2=heifers1.merge(all,how='left',on='WY_id',suffixes=('_x','_y'))
#
heifers3= heifers2.loc[:,('birth_date','dam_num'
# ,'age cow'
# ,'i_date','u_date','readex','age last insem','age last ultra' 
)]
heifers3['age (M))']             =((today - heifers3['birth_date'])/np.timedelta64(1,'M'))
#
heifers4 = heifers3.merge(i6,how='left',on='WY_id',suffixes=('-remove',''))
heifers4.drop([i for i in heifers4.columns if 'remove' in i],axis=1,inplace=True)
heifers4['days last insem']      =(today - heifers4['i_date']).dt.days
#
heifers5 = heifers4.merge(u6,how='left',on='WY_id',suffixes=('-remove',''))
heifers5.drop([i for i in heifers5.columns if 'remove' in i],axis=1,inplace=True)
heifers5['days last ultra']      =(today - heifers5['u_date']).dt.days
#
heifers=heifers5

# Write to csv
#
lb_first.to_csv ('F:\\COWS\\data\\insem_data\\lb_first.csv')
lb_last.to_csv  ('F:\\COWS\\data\\insem_data\\lb_last.csv')
u3.to_csv       ('F:\\COWS\\data\\insem_data\\ultra_last_all.csv')
all.to_csv      ('F:\\COWS\\data\\insem_data\\all.csv')
#
preg.to_csv     ('F:\\COWS\\data\\insem_data\\all_pregnant.csv')
notpreg.to_csv  ('F:\\COWS\\data\\insem_data\\not_pregnant.csv')
tooEarly.to_csv ('F:\\COWS\\data\\insem_data\\ tooEarly.csv')
no_insem.to_csv ('F:\\COWS\\data\\insem_data\\no_insem.csv')
uod.to_csv      ('F:\\COWS\\data\\insem_data\\ultraoutofdate.csv')
#
# notpregHeif.to_csv  ('F:\\COWS\\data\\insem_data\\notpregHeif.csv')
# pregHeif.to_csv     ('F:\\COWS\\data\\insem_data\\pregHeif.csv')
heifers.to_csv         ('F:\\COWS\\data\\insem_data\\all_heifers.csv')
