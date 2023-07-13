import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
# 
lb  =pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv')
bd  =pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv')
u   =pd.read_csv ('F:\\COWS\\data\\csv_files\\ultra.csv')
i   =pd.read_csv ('F:\\COWS\\data\\csv_files\\insem.csv')
s   =pd.read_csv ('F:\\COWS\\data\\csv_files\\stop_dates.csv')
f1  =pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',index_col=0)
# 
f2  =f1.iloc[:,:-5].copy()
# 
lb.set_index('WY_id',inplace=True)
bd.set_index('WY_id',inplace=True)
u.set_index ('WY_id',inplace=True)
i.set_index ('WY_id',inplace=True)
s.set_index ('WY_id',inplace=True)
# 
lb['b_date']=    pd.to_datetime(lb['b_date'],       format='%m/%d/%Y')
bd['death_date']=pd.to_datetime(bd['death_date'],   format='%m/%d/%Y')
bd['birth_date']=pd.to_datetime(bd['birth_date'],   format='%m/%d/%Y')
u['ultra_date']= pd.to_datetime(u['ultra_date'],    format='%m/%d/%Y')
i['insem_date']= pd.to_datetime(i['insem_date'],    format='%m/%d/%Y')
s['stop']=       pd.to_datetime(s['stop'],          format='%m/%d/%Y')
# 
bd_max  = len(bd.index)
index   = list(range(1, bd_max ))
df     = pd.DataFrame(index=index)

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
lb3.index.name = 'WY_id'

#  merge LAST ULTRA WITH reading
u1a=u.groupby('WY_id',as_index=True).agg({
    'Calf_num':'max',
    'ultra_date' :'max'   }).copy()
mcols=['WY_id','Calf_num','ultra_date']
u1b=lb3.merge(u1a,on=('WY_id'),suffixes=('lb','u'))
u1c=u1b.loc[  (u1b['Calf_num'] > u1b['last calf#'])   ]
u1=u1c[['Calf_num','ultra_date']]
# #
u2=u1.merge (right=u,how='left',left_on=mcols,right_on=mcols)
u2.rename   (columns={'Calf_num':'u_calf#','ultra_date':'u_date','ultrareading':'readex'},inplace=True)
# # 
u3a=u2[['u_calf#','u_date','readex']]
u3=df.join(u3a)  #sl=stop_last and is indexed 1-200ish
u3.index.name = 'WY_id'

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
i3.index.name = 'WY_id'

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
all2=x3
all2idx=pd.Series(all2.index.values.tolist())

# calc age of last calf = wks milking
lcb=            all2['lastcalf bdate']
tdy=            pd.to_datetime("today")
wks_milk=       (tdy -lcb) / np.timedelta64(1,'W')
wks_milk.name=  'wks_milk'
all2['wks_milk']=wks_milk

# rearrange columns    
all = all2[['cow bdate'
,'death_date'
,'sl_calf#','stop_last'
,'last calf#','lastcalf bdate'
,'i_calf#','i_date'
,'u_calf#','u_date','readex'
,'stop_next'
,'nextcalf bdate'  ]   ]

#  write to csv
lb_first.to_csv ('F:\\COWS\\data\\insem_data\\lb_first.csv')
lb_last.to_csv  ('F:\\COWS\\data\\insem_data\\lb_last.csv')
all.to_csv     ('F:\\COWS\\data\\insem_data\\status.csv')

