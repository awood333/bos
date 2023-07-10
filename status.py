#!/usr/bin/env python
# coding: utf-8

# In[40]:


import pandas as pd
import numpy as np
import datetime as dt
from datetime import date, datetime, timedelta
#
f1 = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',index_col='datex',parse_dates=['datex'], date_format='%m/%d/%Y')
lb = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv',index_col=None,header=0,parse_dates=['b_date'],dtype=None)
bd = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',index_col=0,header=0,parse_dates=['birth_date','death_date'],low_memory=False,dtype=None)
date_names = ['stop_last','lastcalf bdate','i_date','u_date','next bdate','ultra(e)']
everything = pd.read_csv('F:\\COWS\\data\\insem_data\\all.csv',index_col=0,header=0,parse_dates=date_names,low_memory=False,dtype=None)
arrival = pd.read_csv('F:\\COWS\\data\\csv_files\\arrival.csv',index_col=None,header=0,parse_dates=['arrival_date'],dtype=None)
# 
# set the days in question inclusive of both dates
startdate = pd.to_datetime('6/1/2023')
enddate = pd.to_datetime('6/30/2023')
span = int((enddate - startdate).days + 1)
f = f1.iloc[-span:,:].copy()
# 
# create merged bd/arrival df
bd_merge = bd.merge(arrival, on='WY_id', how='left')
bd_merge['birth_date'] = bd_merge['arrival_date'].combine_first(bd_merge['birth_date'])
bd_merge.drop('arrival_date', axis=1, inplace=True)
#
lb_pivot = lb.pivot_table(
    index= 'WY_id',
    columns='calf#',
    values='b_date')
#
bdidx=      bd.index.to_series()        # integer list of WY_id
idx=        np.arange(1,len(bdidx)+1)   # numpy array
idxforloop= np.arange(0,len(bdidx))
#
datex1 = pd.to_datetime(f.index,format='%m/%d/%Y',errors='coerce')
#
status=     pd.DataFrame()
status['WY_id']=idx
status.set_index('WY_id',inplace=True)
#
bdmax=bd.index.max()+2
rng=list(range(1,bdmax))         #blank df with WYs as index
lastliveheifer=bdmax                #integer, wyid
lastdate=f.index[-1]                # string
df= pd.DataFrame(columns=['status']) 
df['WY_id']=rng
df.set_index('WY_id',drop=True,inplace=True)


# In[57]:


#list of all never-birthed wy's from live_births (pivot)
heif1 = [x for x in bd.index if x not in lb_pivot.index]
#
#filters bd with heif1 mask
heif2 = bd.loc[heif1]
# 
# list comprehension for same thing as in loop below
heif3 = [heif2['death_date'].isnull().sum() for date in f.index]
heif4 = (heif2[heif2['death_date'].isnull()].index.to_list()  for date in f.index)
heif4a = pd.DataFrame(heif4)
heif4a.to_csv('F:\\COWS\\data\\testdata\\heif_list_1.csv')
# 
# 


# In[51]:


diff1 = bd.index[~bd.index.isin(lb_pivot.index)].to_list()
hef1 = bd.loc[diff1]

hef3,hef3a = [],[]
for date in f.index:
    hef2 = (
        hef1['death_date'].isnull().sum()
       )
    hef3.append(hef2)
#
    hef2a = (
        hef1[
        hef1['death_date'].isnull()]
        .index.to_list()
       )
    hef3a.append(hef2a)


hef4a = pd.DataFrame(hef3a)
hef4 = pd.DataFrame(hef3)
hef4a.to_csv('F:\\COWS\\data\\testdata\\heif_list.csv')
hef4.to_csv('F:\\COWS\\data\\testdata\\heif_count.csv')


#  list comprehension version of below

# alive2 = [((bd['birth_date'] < date) & ((bd['death_date'] > date) | bd['death_date'].isnull())).sum() for date in f.index]
# 
# alive_ids2 = [bd.loc[(bd['birth_date'] < date) & ((bd['death_date'].isnull()) | (bd['death_date'] > date))].index for date in f.index]
# 

# In[31]:


milking_count = f.count(axis=1)     #this is the easy part
#
alive2, alive_ids2 = [],[]
for date in f.index:
    alive = (
        (bd['birth_date'] < date) &
        ( (bd['death_date'] > date) |         (bd['death_date'].isnull()))
        ) .sum()
    alive2.append(alive)    
    #
    alive_ids = bd.loc[
        (bd  ['birth_date'] < date)   
        &   ((bd['death_date'].isnull()) | (bd['death_date'] > date ) ) 
            ].index
    alive_ids2.append(alive_ids)
    #
# alive_ids2
# milking_count
# a2= pd.DataFrame(alive2)
a_id2 = pd.DataFrame(alive_ids2)    
# a2.to_csv('F:\\COWS\\data\\testdata\\alive2.csv')
a_id2.to_csv('F:\\COWS\\data\\testdata\\alive_ids2.csv')


# In[27]:


alive_ids


# In[ ]:


datelist = f['datex']
wylist = bdidx  #integers

wylist = f[]
alivecount=[]

for i in datelist:
    alive = []
    (bd[
        ( bd['birth_date'] <= i) & (bd['death_date'] >= i)
        ]
            )
    alivecount.append(x)

alivecount


# In[ ]:


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


# In[ ]:






# In[ ]:


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


# In[13]:


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


# In[14]:


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


# In[80]:


milkingcount=[]
for idx in f.index:
    
    milking=f.loc[idx,:].copy()   
    x=len(milking.loc[milking.notnull()])
    milkingcount.append(x)
  
    
mcount1 = pd.DataFrame(milkingcount,columns=['milking'])
datex = pd.DataFrame(index=f.index)
datex['milking'] = mcount1['milking']

f.loc['06/30/2023']
alivelist


# In[6]:


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


# In[7]:


# Comparison to find duplicates
#
notdry= milkinglist + heiferlist
set_sym=set(alivelist).symmetric_difference(drylist)              #gets elements NOT in common
set_diff=set(alivelist).difference(set(notdry))
set_intersection=set(milkinglist)&set(heiferlist)    #set intersection
#
allx=[*milkinglist,*drylist,*heiferlist]
set_diff2=set(alivelist).difference(set(allx))


# In[8]:


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


# In[9]:


# Box score
#
allcows_count=milking_count+dry_count+heifer_count
cols2=['date','alive_count','milking_count','dry_count','heifer_count','nby_count','dead_count','total']
boxscore=pd.DataFrame(index=None,columns=['type','count'])
#
boxscore['type']=['date','milking_count','dry_count','heifer_count','alive_count','total','nby_count']
boxscore['count'] = [lastdate,milking_count,dry_count,heifer_count,alive_count,allcows_count,nby_count]
boxscore.to_csv('F:\\COWS\\data\\status\\boxscore.csv')


# In[11]:





# In[ ]:




