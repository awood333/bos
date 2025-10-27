'''
nutrition_cows.py
'''

import pandas as pd
import numpy as np


class NutritionCows:
  def __init__(self):
    [self.comp1, self.kg1, 
            self.price1, self.comp,
            
            self.beans_amt1 , self.cassava_amt1, 
            self.corn_amt1 ,self.straw_amt1,
             
            self.beans_price1 , self.cassava_price1, 
            self.corn_price1 ,self.straw_price1
            ]                                     = self.data_loader()
   
    self.latest_feed_kg, self.latest_feed_price   = self.basics()
    self.cost_table                               = self.create_cost_table()
    
  def data_loader(self):
      
    self.comp1  = pd.read_csv    ('F:\\COWS\\data\\feed_data\\nutrition_data\\feedipedia_comp.csv',index_col=0)
    self.kg1    = pd.read_csv    ('F:\\COWS\\data\\feed_data\\nutrition_data\\kg.csv',         index_col=0)
    self.price1 = pd.read_csv    ('F:\\COWS\\data\\feed_data\\nutrition_data\\price.csv',      index_col=0)
    self.comp   = pd.read_csv    ('F:\\COWS\\data\\feed_data\\nutrition_data\\composition_of_feed_WY.csv')
    
    self.beans_amt1     = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\beans_daily_amt.csv')
    self.cassava_amt1   = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\cassava_daily_amt.csv')
    self.corn_amt1      = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\corn_daily_amt.csv')
    self.straw_amt1     = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\straw_daily_amt.csv')


    self.beans_price1     = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\beans_price_seq.csv')
    self.cassava_price1   = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\cassava_price_seq.csv')
    self.corn_price1      = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\corn_price_seq.csv')
    self.straw_price1     = pd.read_csv   ('F:\\COWS\\data\\feed_data\\feed_csv\\straw_price_seq.csv')


    return [self.comp1, self.kg1, 
            self.price1, self.comp,
            
            self.beans_amt1 , self.cassava_amt1, 
            self.corn_amt1 ,self.straw_amt1,
             
            self.beans_price1 , self.cassava_price1, 
            self.corn_price1 ,self.straw_price1
            ]


  def basics(self):
    x=1
    
    beans_amt2    = self.beans_amt1   .iloc[-1:, 1:].copy()
    cassava_amt2  = self.cassava_amt1 .iloc[-1:, 1:].copy()    
    corn_amt2     = self.corn_amt1    .iloc[-1:, 1:].copy()
    straw_amt2    = self.straw_amt1   .iloc[-1:, 1:].copy()    
    
    self.latest_feed_kg = pd.concat([beans_amt2, cassava_amt2, corn_amt2, straw_amt2], axis=0)
    
    
    beans_price2    = self.beans_price1   .iloc[-1].loc['unit_price'].copy()
    cassava_price2  = self.cassava_price1 .iloc[-1].loc['unit_price'].copy()
    corn_price2     = self.corn_price1    .iloc[-1].loc['unit_price'].copy()
    straw_price2    = self.straw_price1   .iloc[-1].loc['unit_price'].copy()

    prices = {
        'beans_price': beans_price2,
        'cassava_price': cassava_price2,
        'corn_price': corn_price2,
        'straw_price': straw_price2
    }
    
    
    latest_feed_price1 = pd.DataFrame(prices,  index=[self.beans_price1.index[-1]])
    self.latest_feed_price  = latest_feed_price1.T
    
    return self.latest_feed_kg, self.latest_feed_price
  
  
  
  def create_cost_table(self):
    kg_np     = np.array(self.latest_feed_kg)
    kg_numeric= kg_np[:,1:].astype(float)
    price_np  = np.array(self.latest_feed_price)
    
    cost_numeric = kg_numeric * price_np
    self.cost_table = np.hstack((kg_np[:, :1], cost_numeric))
    
    return self.cost_table
    
    
if __name__ == "__main__":
  obj=NutritionCows()
  obj.load_and_process()   
    
  # def abc(self):
  #   nutval1 = []
  #   nutval2 = []
  #   nutval3 = []
    
  #   for i in self.compcols:
  #     for j in self.dmcols:
  #       nutval=self.comp[i]*self.dm2.T[i][j]
  #       nutval1.append(nutval)
  #       nutval=[]
  #     nutval2.append(nutval1)
  #     nutval1=[]
  #   nutval3.append(nutval2)

  #   # dataframe
  #   CPFood_DM =    pd.DataFrame(nutval2[0]).fillna(0)     
  #   premix =   pd.DataFrame(nutval2[1]).fillna(0)          
  #   cassava =   pd.DataFrame(nutval2[2]).fillna(0)     
  #   corn =      pd.DataFrame(nutval2[3]).fillna(0)     
  #   molasses =  pd.DataFrame(nutval2[4]).fillna(0)     
  #   ricestraw = pd.DataFrame(nutval2[5]).fillna(0)     
  #   soybean =   pd.DataFrame(nutval2[6]).fillna(0) 
    
  #   bagdryt =    CPFood_DM.T
  #   bagmilkt =   premix.T
  #   cassavat =   cassava.T
  #   cornt =      corn.T
  #   molassest =  molasses.T
  #   ricestrawt = ricestraw.T
  #   soybeant =   soybean.T
 
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

