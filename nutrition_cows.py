import pandas as pd
import numpy as np
#x
comp1=  pd.read_csv   ('F:\\COWS\\data\\nutrition_data\\composition.csv',index_col=0)
kg1=    pd.read_csv     ('F:\\COWS\\data\\nutrition_data\\kg.csv',index_col=0)
price1= pd.read_csv     ('F:\\COWS\\data\\nutrition_data\\price.csv',index_col=0)

comp=comp1.iloc[:,:].copy()
dmname=comp.index[0]
comp.rename(index={dmname:'DM kg'},inplace=True)
dm1=comp1.iloc[:1,:].squeeze()
#
price=price1.iloc[:,:].squeeze()
#
dm2=kg1.mul(dm1,axis=0)
compcols=list(comp.columns)
dmcols=list(dm2.columns)
#
nutval1=[]
nutval2=[]
nutval3=[]
#
for i in compcols:
  for j in dmcols:
    nutval=comp[i]*dm2.T[i][j]
    nutval1.append(nutval)
    nutval=[]
  nutval2.append(nutval1)
  nutval1=[]
nutval3.append(nutval2)

# dataframe
bagdry=    pd.DataFrame(nutval2[0]).fillna(0)     
bagmilk=   pd.DataFrame(nutval2[1]).fillna(0)          
cassava=   pd.DataFrame(nutval2[2]).fillna(0)     
corn=      pd.DataFrame(nutval2[3]).fillna(0)     
molasses=  pd.DataFrame(nutval2[4]).fillna(0)     
ricestraw= pd.DataFrame(nutval2[5]).fillna(0)     
soybean=   pd.DataFrame(nutval2[6]).fillna(0) 
#
bagdryt=    bagdry.T
bagmilkt=   bagmilk.T
cassavat=   cassava.T
cornt=      corn.T
molassest=  molasses.T
ricestrawt= ricestraw.T
soybeant=   soybean.T
#
colnames= ['fresh','peak','late','dry','close']
bagdryt     .columns=colnames
bagmilkt    .columns=colnames
cassavat    .columns=colnames
cornt       .columns=colnames
molassest   .columns=colnames
ricestrawt  .columns=colnames
soybeant    .columns=colnames
#
rations=bagdryt + bagmilkt + cassavat + cornt + molassest + ricestrawt + soybeant
#
bodywt=450
bodywtpcts=rations.divide(bodywt)
#
dm=rations.loc['DM kg']
rationsDM=rations.divide(dm)

#
# cost (adjust weeks / group here)
feedduration={'fresh':2,'peak':13,'late':29,'dry':6,'close':2}
feedduration.update((key,value*7)for key, value in feedduration.items())
feed_dur=pd.Series(feedduration,name='days')
#
kgt=        kg1.T
feedkg=     kgt.multiply(feed_dur,axis=0)
feedsum=    feedkg.sum(axis=0)
feedkgsum=  feedkg.sum(axis=0)
#
feedkg.loc['total']=feedkg.sum(axis=0)
feedcost=   feedsum*price
feedcost.loc['total']=feedcost.sum(axis=0)
#
x=        pd.concat([feedkgsum,feedcost],axis=1)
x.columns=  ('kg','value')


# write to csv
rations     .to_csv('F:\\COWS\\data\\nutrition_data\\output\\rations.csv')
bodywtpcts  .to_csv('F:\\COWS\\data\\nutrition_data\\output\\bodywtpcts.csv')                    
bagdryt     .to_csv('F:\\COWS\\data\\nutrition_data\\output\\bagdry.csv')
bagmilkt    .to_csv('F:\\COWS\\data\\nutrition_data\\output\\bagmilk.csv')
cassavat    .to_csv('F:\\COWS\\data\\nutrition_data\\output\\cassava.csv')
cornt       .to_csv('F:\\COWS\\data\\nutrition_data\\output\\corn.csv')
molassest   .to_csv('F:\\COWS\\data\\nutrition_data\\output\\molasses.csv')
ricestrawt  .to_csv('F:\\COWS\\data\\nutrition_data\\output\\ricestraw.csv')
soybeant    .to_csv('F:\\COWS\\data\\nutrition_data\\output\\soybean.csv')
#
feedkg      .to_csv('F:\\COWS\\data\\nutrition_data\\output\\feedkg.csv')
x           .to_csv('F:\\COWS\\data\\nutrition_data\\output\\cost_kg.csv')
rationsDM   .to_csv('F:\\COWS\\data\\nutrition_data\\output\\rationsDM.csv')
# .to_csv('F:\\COWS\\data\\nutrition_data\\output\\.csv')