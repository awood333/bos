'''
nutrition_cows.py
'''

import pandas as pd
import numpy as np


class NutritionCows:
  def __init__(self):
    self.comp1  = pd.read_csv    ('F:\\COWS\\data\\nutrition_data\\composition.csv',index_col=0)
    self.kg1    = pd.read_csv    ('F:\\COWS\\data\\nutrition_data\\kg.csv',         index_col=0)
    self.price1 = pd.read_csv    ('F:\\COWS\\data\\nutrition_data\\price.csv',      index_col=0)
    self.comp   = pd.read_csv    ('F:\\COWS\\data\\feed_data\\nutrition\\composition_of_feed.csv')
    
    #  functions
    self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcols = self.basics()
    
    
  def basics(self):

    self.comp    = self.comp1.iloc[:,:].copy()
    self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcolsdmname  = self.comp.index[0]
    
    self.comp.rename(index={self.dmname:'DM kg'},inplace=True)
    self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcolsdm1     = self.comp1.iloc[:1,:].squeeze()
    self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcolsprice   = self.price1.iloc[:,:].squeeze()
 
    self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcolsdm2       = self.kg1.mul(self.dm1,axis=0)
    self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcolscompcols  = list(self.comp.columns)
    self.dmcols    = list(self.dm2.columns)
    
    return self.comp, self.dmname, self.dm1, self.price, self.dm2, self.compcols, self.dmcols 
 
 
  def abc(self):
    nutval1 = []
    nutval2 = []
    nutval3 = []
    
    for i in self.compcols:
      for j in self.dmcols:
        nutval=self.comp[i]*self.dm2.T[i][j]
        nutval1.append(nutval)
        nutval=[]
      nutval2.append(nutval1)
      nutval1=[]
    nutval3.append(nutval2)

    # dataframe
    CPFood_DM =    pd.DataFrame(nutval2[0]).fillna(0)     
    premix =   pd.DataFrame(nutval2[1]).fillna(0)          
    cassava =   pd.DataFrame(nutval2[2]).fillna(0)     
    corn =      pd.DataFrame(nutval2[3]).fillna(0)     
    molasses =  pd.DataFrame(nutval2[4]).fillna(0)     
    ricestraw = pd.DataFrame(nutval2[5]).fillna(0)     
    soybean =   pd.DataFrame(nutval2[6]).fillna(0) 
    
    bagdryt =    CPFood_DM.T
    bagmilkt =   premix.T
    cassavat =   cassava.T
    cornt =      corn.T
    molassest =  molasses.T
    ricestrawt = ricestraw.T
    soybeant =   soybean.T
 
# colnames= ['fresh','peak','late','dry','close']
# bagdryt     .columns=colnames
# bagmilkt    .columns=colnames
# cassavat    .columns=colnames
# cornt       .columns=colnames
# molassest   .columns=colnames
# ricestrawt  .columns=colnames
# soybeant    .columns=colnames
 
# rations = bagdryt + bagmilkt + cassavat + cornt + molassest + ricestrawt + soybeant
 
# bodywt = 450
# bodywtpcts = rations.divide(bodywt)
 
# dm = rations.loc['DM kg']
# rationsDM = rations.divide(dm)

 
# # cost (adjust weeks / group here)
# feedduration = {'fresh':2,'peak':13,'late':29,'dry':6,'close':2}
# feedduration.update((key,value*7)for key, value in feedduration.items())
# feed_dur = pd.Series(feedduration,name='days')
 
# kgt =        kg1.T
# feedkg =     kgt.multiply(feed_dur,axis=0)
# feedsum =    feedkg.sum(axis=0)
# feedkgsum =  feedkg.sum(axis=0)
 
# feedkg.loc['total']=feedkg.sum(axis=0)
# feedcost =   feedsum*price
# feedcost.loc['total']=feedcost.sum(axis=0)
 
# x =        pd.concat([feedkgsum,feedcost],axis=1)
# x.columns =  ('kg','value')


# # write to csv
# rations     .to_csv('F:\\COWS\\data\\nutrition_data\\output\\rations.csv')
# bodywtpcts  .to_csv('F:\\COWS\\data\\nutrition_data\\output\\bodywtpcts.csv')                    
# bagdryt     .to_csv('F:\\COWS\\data\\nutrition_data\\output\\CPFood_DM.csv')
# bagmilkt    .to_csv('F:\\COWS\\data\\nutrition_data\\output\\premix.csv')
# cassavat    .to_csv('F:\\COWS\\data\\nutrition_data\\output\\cassava.csv')
# cornt       .to_csv('F:\\COWS\\data\\nutrition_data\\output\\corn.csv')
# molassest   .to_csv('F:\\COWS\\data\\nutrition_data\\output\\molasses.csv')
# ricestrawt  .to_csv('F:\\COWS\\data\\nutrition_data\\output\\ricestraw.csv')
# soybeant    .to_csv('F:\\COWS\\data\\nutrition_data\\output\\soybean.csv')
# #
# feedkg      .to_csv('F:\\COWS\\data\\nutrition_data\\output\\feedkg.csv')
# x           .to_csv('F:\\COWS\\data\\nutrition_data\\output\\cost_kg.csv')
# rationsDM   .to_csv('F:\\COWS\\data\\nutrition_data\\output\\rationsDM.csv')
# .to_csv('F:\\COWS\\data\\nutrition_data\\output\\.csv')

if __name__ == "__main__":
  NutritionCows()