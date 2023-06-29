import pandas as pd    #123   456
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as td
##
start=pd.read_csv   ('F:\\COWS\\data\\csv_files\\live_births.csv',header=0,parse_dates=['b_date'])
stop=pd.read_csv    ('F:\\COWS\\data\\csv_files\\stop_dates.csv',header=0,parse_dates=['stop'])
milk=pd.read_csv    ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',header=0,index_col=0)
step7=pd.read_csv   ('F:\\COWS\\data\\testdata\\step7_series.csv')
col_list=milk.columns               #fullday labels are 'object'
#
col_list_int=[int(x) for x in milk.columns]
milk_a=milk.loc[:,col_list].copy()
#
step7.rename(columns={'wk':666},inplace=True)

#   create an array of blank rows to pad the milk df back to 9/1/2016<br>
#
startday=   pd.to_datetime('2016-09-01')
endday=     pd.to_datetime(milk.index.min())    
blankdays=  (endday-startday).days  
cols=       len(milk_a.columns)
colnames=   milk_a.columns
milkfill=   np.zeros([blankdays,cols])                  
#         
milkx1=     np.array(milk_a,dtype=float)          #this turns the milk df into an array
milkx=      np.row_stack([milkfill,milkx1])       #and joins with the zeros array from above
#
milk1=      pd.DataFrame(milkx)   
mmax=       pd.to_datetime(milk.index.max())        
datex1=     pd.date_range(start=startday,end=mmax,freq='D',name='datex') 
#
milk1['datex']=datex1
milk1.set_index('datex',drop=True,inplace=True)
milk1.columns=col_list
milk1.replace(0,np.nan,inplace=True)            #milk1 is in register with fullday - the wy's are lined up correctly

# align the start and stop dfs (they're different lengths)<br>
startpivot=start.pivot(index='WY_id',columns='calf#',values='b_date')   #128,6
stoppivot=stop.pivot(index='WY_id',columns='lact_num',values='stop')    #118,6
rng1=np.arange(0,201,dtype=int)                         #for wy numbers - col headings
rng=pd.Series(rng1,name='WY_id')
start2=startpivot.merge(right=rng,how='right',on='WY_id')
stop2=stoppivot.merge(right=rng,how='right',on='WY_id')
#
start2.set_index('WY_id',inplace=True)
stop2.set_index('WY_id',inplace=True)
#
startT=start2.T.astype(dtype='datetime64[ns]')      #6,200
stopT=stop2.T.astype(dtype='datetime64[ns]')        #6,200
#
startTx=startT.loc[:,:]
stopTx=stopT.loc[:,:]

# LOOP
z=  np.zeros((1200,0))
wk= np.array(step7[666])
z=  np.column_stack((z,wk))
#
milk3,milk3a,milksum3,milkmax3,milk4,milksum4,milkmax4,milk2idx3,milk2idx4,milk4a, milk5,milk5a,milk6,milk6a=[],[],[],[],[],[],[],[],[],[],[],[],[],[]
milkmean3,milkcount3,milkmean4,milkcount4=[],[],[],[]
wy,start4,stop4,r2,c2 = [],[],[],[],[]
#
rows1=stopTx.index    # string 'objects'
rows=range(1,7)       #integers
cols=stopTx.columns    #integers
#
#
#   NOTE: milk1 takes a string col_name, start/stop take integer col labels
#
for r in rows:           
    for c in cols:        
      c1=str(c)
      r1='L'+str(r)
      start4=startTx.loc[r,c]
      stop4=stopTx.loc[r,c]
      if   (pd.isnull(start4) == False) & (pd.isnull(stop4) == False)   :
        milk2   =milk1.loc[start4:stop4,c1]
        milk2a  =(0,0)
        c2.append(c1)
        r2.append(r1)
        milk3   =np.array(milk2)
        milk3a  =np.array(milk2a)
        gap1     =z.shape[0] - milk3.shape[0]
        gap2     =z.shape[0] - milk3a.shape[0]
        milk4   =np.pad(milk3,pad_width=((0,gap1)),constant_values=0)
        milk4a  =np.pad(milk3a,pad_width=((0,gap2)),constant_values=0)
        milk5.append(milk4)
        milk5a.append(milk4a)      
        milk2,milk2a=[],[]
#     
      elif   ((pd.isnull(start4) == False)    & (pd.isnull(stop4) == True))  :
        stop4=mmax   
        c1=str(c)
        r1='L'+str(r)
        c2.append(c1)
        r2.append(r1) 
        milk2   =(0,0)
        milk2a  =milk1.loc[start4:stop4,c1]
        c2.append(c1)
        r2.append(r1)
        milk3   =np.array(milk2)
        milk3a  =np.array(milk2a)
        gap1     =z.shape[0] - milk3.shape[0]
        gap2     =z.shape[0] - milk3a.shape[0]
        milk4   =np.pad(milk3,pad_width=((0,gap1)),constant_values=0)
        milk4a  =np.pad(milk3a,pad_width=((0,gap2)),constant_values=0)
        milk5.append(milk4)
        milk5a.append(milk4a)      
        milk2,milk2a=[],[]
#        
      elif   ((pd.isnull(start4) == True)    & (pd.isnull(stop4) == True))  :
        c2.append(c1)
        r2.append(r1)
        milk2   =(0,0)
        c2.append(c1)
        r2.append(r1)
        milk3   =np.array(milk2)
        gap     =z.shape[0] - milk3.shape[0]
        milk4   =np.pad(milk3,pad_width=((0,gap)),constant_values=0)
        milk5.append(milk4)
        milk5a.append(milk4)
    
        milk2=[]
    milk6.append(milk5)
    milk6a.append(milk5a)
    milk5,milk5a=[],[]
#
milkx=np.array(milk6)
milkxa=np.array(milk6a)
#

# still milking agg

lacx1=milkxa[0]
lacx2=milkxa[1]
lacx3=milkxa[2]
lacx4=milkxa[3]
lacx5=milkxa[4]
lacx6=milkxa[5]
k1=list(range(0,(lacx6.shape[1])))
lacxidx=list(range(0,stopTx.shape[1]))
#
lacx1[np.isnan(lacx1)]=0
lacx2[np.isnan(lacx2)]=0
lacx3[np.isnan(lacx3)]=0
lacx4[np.isnan(lacx4)]=0
lacx5[np.isnan(lacx5)]=0
lacx6[np.isnan(lacx6)]=0
#
laclist=(lacx1,lacx2,lacx3,lacx4,lacx5)
milking1=sum(laclist)
milking2=np.sum(milking1,axis=1,dtype=float)
milking=pd.DataFrame(milking2)
milking.rename(columns={0:'M'},inplace=True)
#

# daily aggregations

#
lac1=milkx[0]
lac2=milkx[1]
lac3=milkx[2]
lac4=milkx[3]
lac5=milkx[4]
lac6=milkx[5]
k1=list(range(0,(lac6.shape[1])))
lacidx=list(range(0,stopTx.shape[1]))
#
lact1=pd.DataFrame(lac1,index=lacidx,columns=k1)   #1200
lact2=pd.DataFrame(lac2,index=lacidx,columns=k1)
lact3=pd.DataFrame(lac3,index=lacidx,columns=k1)
lact4=pd.DataFrame(lac4,index=lacidx,columns=k1)
lact5=pd.DataFrame(lac5,index=lacidx,columns=k1)
lact6=pd.DataFrame(lac6,index=lacidx,columns=k1)
#
lac1sum   =lact1.sum(axis=1,skipna=True)  
lac2sum   =lact2.sum(axis=1,skipna=True)  
lac3sum   =lact3.sum(axis=1,skipna=True)  
lac4sum   =lact4.sum(axis=1,skipna=True)  
lac5sum   =lact5.sum(axis=1,skipna=True)
stats_sum=pd.concat([lac1sum,lac2sum,lac3sum,lac4sum,lac5sum],axis=1)  
#
lact1.replace(0,np.nan,inplace=True)
lact2.replace(0,np.nan,inplace=True)
lact3.replace(0,np.nan,inplace=True)
lact4.replace(0,np.nan,inplace=True)
lact5.replace(0,np.nan,inplace=True)
#
lact1count=lact1.count(axis=1)
lact2count=lact2.count(axis=1)
lact3count=lact3.count(axis=1)
lact4count=lact4.count(axis=1)
lact5count=lact5.count(axis=1)
#
lact1.fillna(0,inplace=True)
lact2.fillna(0,inplace=True)
lact3.fillna(0,inplace=True)
lact4.fillna(0,inplace=True)
lact5.fillna(0,inplace=True)
lact6.fillna(0,inplace=True)
#
lact1.index 
lact2.index 
lact3.index 
lact4.index
lact5.index 
lact6.index 
#
lact1.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact1.csv')
lact2.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact2.csv')
lact3.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact3.csv')
lact4.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact4.csv')
lact5.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact5.csv')
milking.to_csv('F:\\COWS\\data\\milk_data\\lactations\\milking.csv')
#
lact1count.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact1count.csv')
lact2count.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact2count.csv')
lact3count.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact3count.csv')
lact4count.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact4count.csv')
lact5count.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact5count.csv')

#   WEEKLY AGG

# lac1
wk1=lact1.T.merge(step7,left_index=True,right_index=True)
wk2=wk1.loc[:307,:].copy()
wk_L1=wk2.groupby([666],as_index=True).mean()
wk_L1.drop(['n1','step7'],axis=1,inplace=True)
# wk_L1.replace(0,np.nan,inplace=True)
wk_L1.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\wk_L1.csv')
#
# lac2
wk1=lact2.T.merge(step7,left_index=True,right_index=True)
wk2=wk1.loc[:307,:].copy()
wk_L2=wk2.groupby([666],as_index=True).mean()
wk_L2.drop(['n1','step7'],axis=1,inplace=True)
# wk_L2.replace(0,np.nan,inplace=True)
wk_L2.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\wk_L2.csv')
#
# lac3
wk1=lact3.T.merge(step7,left_index=True,right_index=True)
wk2=wk1.loc[:307,:].copy()
wk_L3=wk2.groupby([666],as_index=True).mean()
wk_L3.drop(['n1','step7'],axis=1,inplace=True)
# wk_L3.replace(0,np.nan,inplace=True)
wk_L3.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\wk_L3.csv')
#
# lac4
wk1=lact4.T.merge(step7,left_index=True,right_index=True)
wk2=wk1.loc[:307,:].copy()
wk_L4=wk2.groupby([666],as_index=True).mean()
wk_L4.drop(['n1','step7'],axis=1,inplace=True)
# wk_L4.replace(0,np.nan,inplace=True)
wk_L4.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\wk_L4.csv')
#
# lac5
wk1=lact5.T.merge(step7,left_index=True,right_index=True)
wk2=wk1.loc[:307,:].copy()
wk_L5=wk2.groupby([666],as_index=True).mean()
wk_L5.drop(['n1','step7'],axis=1,inplace=True)
# wk_L5.replace(0,np.nan,inplace=True)
wk_L5.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\wk_L5.csv')

#   aggregations  

#
#   Mean
#
lac1mean   =wk_L1.mean(axis=0,skipna=True)  
lac2mean   =wk_L2.mean(axis=0,skipna=True)  
lac3mean   =wk_L3.mean(axis=0,skipna=True)  
lac4mean   =wk_L4.mean(axis=0,skipna=True)  
lac5mean   =wk_L5.mean(axis=0,skipna=True) 
#
stats_mean=pd.concat([lac1mean,lac2mean,lac3mean,lac4mean,lac5mean],axis=1)
#
#   Max
#
lac1max   =wk_L1.max(axis=0,skipna=True)  
lac2max   =wk_L2.max(axis=0,skipna=True)  
lac3max   =wk_L3.max(axis=0,skipna=True)  
lac4max   =wk_L4.max(axis=0,skipna=True)  
lac5max   =wk_L5.max(axis=0,skipna=True) 
#
stats_max=pd.concat([lac1max,lac2max,lac3max,lac4max,lac5max],axis=1)

#  lac_summary
#
colsx=['sum','count','mean','max']
lac1summary=pd.concat([lac1sum,lact1count,lac1mean,lac1max],axis=1)
lac2summary=pd.concat([lac2sum,lact2count,lac2mean,lac2max],axis=1)
lac3summary=pd.concat([lac3sum,lact3count,lac3mean,lac3max],axis=1)
lac4summary=pd.concat([lac4sum,lact4count,lac4mean,lac4max],axis=1)
lac5summary=pd.concat([lac5sum,lact5count,lac5mean,lac5max],axis=1)
#
lac1summary.columns=colsx
lac2summary.columns=colsx
lac3summary.columns=colsx
lac4summary.columns=colsx
lac5summary.columns=colsx
#
lac1summary.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lac_summary\\lac1summary.csv')
lac2summary.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lac_summary\\lac2summary.csv')
lac3summary.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lac_summary\\lac3summary.csv')
lac4summary.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lac_summary\\lac4summary.csv')
lac5summary.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lac_summary\\lac5summary.csv')

# write to csv
#
stats_sum.columns=('L1','L2','L3','L4','L5')
stats_sum.replace(0,np.nan,inplace=True)
stats_sum.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lactation_stats_sum.csv')
#
stats_mean.columns=('L1','L2','L3','L4','L5')
stats_mean.replace(0,np.nan,inplace=True)
stats_mean.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lactation_stats_mean.csv')
#
stats_max.columns=('L1','L2','L3','L4','L5')
stats_max.replace(0,np.nan,inplace=True)
stats_max.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lactation_stats_max.csv')
