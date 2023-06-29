import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
#
f=pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
bd=pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',index_col=0,header=0,parse_dates=['birth_date','death_date'],low_memory=False,dtype=None)
date_names = ['age cow','stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything=pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',index_col=0,header=0,parse_dates=date_names, date_format='%m/%d/%Y',low_memory=False,dtype=None)
#
bdidx=      bd.index.to_series()
idx=        np.arange(1,len(bdidx)+1)
idxforloop= np.arange(0,len(bdidx))
#
status=     pd.DataFrame()
status['WY_id']=idx
status.set_index('WY_id',inplace=True)
#
bd['birth_date']=pd.to_datetime(bd['birth_date'],)
bd['death_date']=pd.to_datetime(bd['death_date'],)
everything['cow bdate']=bd['birth_date']
everything['death_date']=bd['death_date']
# 
bdmax=bd.index.max()+2
rng=list(range(1,bdmax))         #blank df with WYs as index
lastliveheifer=bdmax
lastdate=f.iloc[-1:,0].to_string(index=False)
df= pd.DataFrame(columns=['status']) 
df['WY_id']=rng
df.set_index('WY_id',drop=True,inplace=True)

# Alive / dead
bd2=bd.iloc[:lastliveheifer,:].copy()
alive=bd2.loc[
    (bd2['death_date'].isnull()
    )    ].copy()
alive['alive']='alive'
alivelist=list(alive.index.values)
alivelist.sort()
alivex=pd.DataFrame(alivelist,columns=['alive'])
#
dead=bd.loc[(bd['death_date'].notnull())    ].copy()
dead['dead']='dead'
deadlist=dead.index.tolist()
#
alive_count=len(alivelist)
dead_count=len(deadlist)
df.loc[deadlist] ='gone'
# Heifers
#
everything2=everything.iloc[:lastliveheifer,:].copy()
heifers = everything2.loc[(
        everything2['death_date'].isnull()
    &   everything2['stop_last'].isnull()
    &   everything2['lastcalf bdate'].isnull()
    )].copy()
# 
heiferlist=heifers.index.tolist()
heiferlist.sort()
heiferx=pd.DataFrame(heiferlist,columns=['heifer'])
heifer_count=len(heiferlist)
df.loc[heiferlist]= 'H'

# NBY
#
lastday1=f.iloc[-1,0]
lastday=pd.to_datetime(lastday1)
#
nby=everything.loc[(
    everything['cow bdate'] > lastday
    )].copy()
#
nbylist=nby.index.tolist()
nbyx=pd.DataFrame(nbylist,columns=['nby'])
nbylist.sort()
nby_count=len(nbylist)

# Milking cows on last milking day on record
#
milking=f.T.iloc[1:,-1] .copy()   
milking.index.name='WY_id'
milkinglistx=[]
for i in milking.index:
    if milking[i] >0:
        milkinglistx.append(i)
    elif milking[i]==0:
        pass
milkinglist=[int(x) for x in milkinglistx]
milkinglist.sort()
milking_count=len(milkinglist)
df.loc[milkinglist]='M'
milkx=pd.DataFrame(milkinglist,columns=['milking'])

# Dry
#
notdry= milkinglist + heiferlist
dry1=set(alivelist).difference(set(notdry))    #set diff
drylist=list(dry1)                                  # sets have a problem
drylist.sort()
dryx=pd.DataFrame(drylist)
dryx.rename(columns={0:'dry'},inplace=True)
dry_count=len(drylist)
df.loc[drylist]='dry'

# Comparison to find duplicates
#
notdry= milkinglist + heiferlist
set_sym=set(alivelist).symmetric_difference(drylist)              #gets elements NOT in common
set_diff=set(alivelist).difference(set(notdry))
set_intersection=set(milkinglist)&set(heiferlist)    #set intersection
#
allx=[*milkinglist,*drylist,*heiferlist]
set_diff2=set(alivelist).difference(set(allx))

# Status df
#
collist=[alivex,milkx,dryx,heiferx,nbyx]
sl1=pd.merge(left=alivex,right=milkx,how='outer',left_index=True,right_index=True)
sl2=pd.merge(left=sl1,right=dryx,how='outer',left_index=True,right_index=True)
sl3=pd.merge(left=sl2,right=heiferx,how='outer',left_index=True,right_index=True)
sl4=pd.merge(left=sl3,right=nbyx,how='outer',left_index=True,right_index=True)
#
sl4.to_csv('F:\\COWS\\data\\status\\status_lists.csv')
df.to_csv('F:\\COWS\\data\\status\\status_column.csv')

# Box score
#
allcows_count=milking_count+dry_count+heifer_count
cols2=['date','alive_count','milking_count','dry_count','heifer_count','nby_count','dead_count','total']
boxscore=pd.DataFrame(index=None,columns=['type','count'])
#
boxscore['type']=['date','milking_count','dry_count','heifer_count','alive_count','total','nby_count']
boxscore['count'] = [lastdate,milking_count,dry_count,heifer_count,alive_count,allcows_count,nby_count]
boxscore.to_csv('F:\\COWS\\data\\status\\boxscore.csv')
