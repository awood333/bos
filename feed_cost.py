'''
feed_cost.py
'''
import pandas as pd
import os
from status import StatusData
from startdate_funct import CreateStartdate


class FeedCost:
    def __init__(self):
            
        self.status = StatusData()
        self.csd = CreateStartdate()
        
        self.f1  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',
                        index_col=0, header=0, parse_dates=['datex'])

        self.maxdate     = self.f1.index.max()   
        self.stopdate    = self.maxdate
        self.startdate   = self.csd.startdate
        self.date_format = self.csd.date_format
        self.date_range  = pd.date_range(self.startdate, self.stopdate, freq='D')

        self.f = self.f1.loc[self.date_range].copy()    #partition the milk dbase
    
        #functions
        self.cassava_cost                       = self.create_cassava_cost()
        self.bean_cost                          = self.create_beans_cost()
        self.corn_cost                          = self.create_corn_cost()
        self.straw_cost                         = self.create_straw_cost()

        self.total_feedcost                     = self.create_total_feedcost()
        self.monthly_feedcost                   = self.create_monthly_feedcost()
        self.weekly_feedcost                    = self.create_weekly_feedcost()
        
        self.group_a_daily_feed_costpercow      = self.create_group_a_daily_feed_costpercow()
        self.group_b_daily_feed_costpercow      = self.create_group_b_daily_feed_costpercow()
        self.dry_daily_feed_costpercow          = self.create_dry_daily_feed_costpercow()
        self.latest_daily_feed_costpercow       = self.create_latest_daily_feed_costpercow()
        self.create_write_to_csv()
        
    def create_cassava_cost(self):
        return create_feed_cost(self, 'cassava')
    
    def create_beans_cost(self):
        return create_feed_cost(self, 'beans')
        
    def create_corn_cost(self):
        return create_feed_cost(self, 'corn')

    def create_straw_cost(self):
        return create_feed_cost(self, 'straw')                              

        
    def create_group_a_daily_feed_costpercow(self):
        group_a_daily_feed_costpercow = pd.DataFrame({
             'corn'     : self.corn_cost    ['group_a_costpercow']   , 
             'cassava'  : self.cassava_cost ['group_a_costpercow']   , 
             'bean'     : self.bean_cost    ['group_a_costpercow']   ,
             'straw'    : self.straw_cost   ['group_a_costpercow']
             })
        
        group_a_daily_feed_costpercow['b_total'] =group_a_daily_feed_costpercow.sum(axis=1)
        return group_a_daily_feed_costpercow
    
    
    

    def create_group_b_daily_feed_costpercow(self):
        group_b_daily_feed_costpercow = pd.DataFrame({
             'corn'     : self.corn_cost['group_b_costpercow']      , 
             'cassava'  : self.cassava_cost['group_b_costpercow']   , 
             'bean'     : self.bean_cost['group_b_costpercow']      ,
             'straw'    : self.straw_cost['group_b_costpercow']
             })
        
        group_b_daily_feed_costpercow['b_total'] =group_b_daily_feed_costpercow.sum(axis=1)
        return group_b_daily_feed_costpercow
    
    
    
    def create_dry_daily_feed_costpercow(self):
        dry_daily_feed_costpercow = pd.DataFrame({
            'corn'      : self.corn_cost['dry_costpercow']      , 
            'cassava'   : self.cassava_cost['dry_costpercow']   , 
            'bean'      : self.bean_cost['dry_costpercow']      , 
            'straw'     : self.straw_cost['dry_costpercow']
            })
        
        dry_daily_feed_costpercow['dry_total'] =dry_daily_feed_costpercow.sum(axis=1)
        # ddfc = dry_daily_feed_costpercow.iloc[-1:,:]
        return dry_daily_feed_costpercow




    def create_total_feedcost(self):
        tfc = pd.DataFrame()
        tfc['beans']    = self.bean_cost    ['total daily cost']
        tfc['cassava']  = self.cassava_cost ['total daily cost']
        tfc['corn']     = self.corn_cost    ['total daily cost']
        tfc['straw']    = self.straw_cost   ['total daily cost']
        tfc['total feed cost'] = (self.bean_cost['total daily cost'] 
                                  + self.cassava_cost['total daily cost'] 
                                  + self.corn_cost['total daily cost'] 
                                  + self.straw_cost['total daily cost'])
        
        tfc['year']     = tfc.index.year
        tfc['month']    = tfc.index.month
        
        total_feedcost = pd.DataFrame(tfc)
        return total_feedcost



    def create_monthly_feedcost(self):
        tfc = self.total_feedcost.copy()
        tfc_m = tfc.groupby(['year', 'month']).agg({
            'beans'     : 'sum', 
            'cassava'   : 'sum',
            'corn'      : 'sum',
            'straw'     : 'sum',
            'total feed cost': 'sum'
        })

        monthly_feedcost = tfc_m
        return monthly_feedcost
    
    
    
    
    def create_weekly_feedcost(self):
                
        bean1           = self.bean_cost.copy()
        cassava1        = self.cassava_cost.copy()
        corn1           = self.corn_cost.copy()
        straw1          = self.straw_cost.copy()

        bean1['year']   = bean1.index.year
        bean1['month']  = bean1.index.month
        bean1['week']   = bean1.index.isocalendar().week


        cassava1['year']    = cassava1.index.year
        cassava1['month']   = cassava1.index.month
        cassava1['week']    = cassava1.index.isocalendar().week


        corn1['year']   = corn1.index.year
        corn1['month']  = corn1.index.month
        corn1['week']   = corn1.index.isocalendar().week
        
        straw1['year']   = straw1.index.year
        straw1['month']  = straw1.index.month
        straw1['week']   = straw1.index.isocalendar().week

        bean2       = bean1.    drop(columns=['feed_type_x', 'feed_type_y'])
        cassava2    = cassava1. drop(columns=['feed_type_x', 'feed_type_y'])
        corn2       = corn1.    drop(columns=['feed_type_x', 'feed_type_y'])
        straw2      = straw1.    drop(columns=['feed_type_x','feed_type_y'])

        bean3       = bean2.    groupby(['year','month','week'],   as_index=False).mean()
        cassava3    = cassava2. groupby(['year','month','week'],   as_index=False).mean()
        corn3       = corn2.    groupby(['year','month','week'],   as_index=False).mean()
        straw3      = straw2.    groupby(['year','month','week'],  as_index=False).mean()

        bean4       = bean3     ['total daily cost'].to_frame()        # [['milkers', 'dry_count', 'milk+dry','total daily cost']]
        cassava4    = cassava3  ['total daily cost'].to_frame()
        corn4       = corn3     ['total daily cost'].to_frame()
        straw4      = straw3    ['total daily cost'].to_frame()
        
        bean4.rename(columns=   {'total daily cost': 'cost beans'},     inplace=True)
        cassava4.rename(columns={'total daily cost': 'cost cassava'},   inplace=True)
        corn4.rename(columns=   {'total daily cost': 'cost corn'},      inplace=True)
        straw4.rename(columns=  {'total daily cost': 'cost straw'},     inplace=True)
        
        all_feed1   = bean4.merge(    cassava4,  how='left', left_index=True, right_index=True)
        all_feed2    = all_feed1.merge(  corn4,  how='left', left_index=True, right_index=True)
        all_feed    = all_feed2.merge(   straw4, how='left', left_index=True, right_index=True)
        
        all_feed['total feed cost'] = (  all_feed   ['cost beans'] 
                                       + all_feed   ['cost cassava'] 
                                       + all_feed   ['cost corn'] 
                                       + all_feed   ['cost straw'])
        weekly_feedcost = all_feed

        return weekly_feedcost
    
    
    def create_latest_daily_feed_costpercow(self):
        ddfc = self.dry_daily_feed_costpercow.iloc[-1:,:].copy()
        ddfc.rename(columns={'dry_total': 'total'}, inplace=True)
        
        
        ddga = self.group_a_daily_feed_costpercow.iloc[-1:,:].copy()
        ddga.rename(columns={'a_total': 'total'}, inplace=True)
        
        ddgb = self.group_b_daily_feed_costpercow.iloc[-1:,:].copy()
        ddgb.rename(columns={'b_total': 'total'}, inplace=True)
        
        
        name_col = ['dry', 'group a', 'group b']
        
        latest_daily_feed_costpercow = pd.concat([ddfc, ddga, ddgb])
        latest_daily_feed_costpercow['group'] = name_col
        latest_daily_feed_costpercow['liter equiv'] = latest_daily_feed_costpercow['total'] / 21.25
        
        return latest_daily_feed_costpercow
    
    



    def create_write_to_csv(self):
        self.corn_cost          .to_csv('F:\\COWS\\data\\feed_data\\corn_cost.csv')
        self.cassava_cost       .to_csv('F:\\COWS\\data\\feed_data\\cassava_cost.csv')
        self.bean_cost          .to_csv('F:\\COWS\\data\\feed_data\\beans_cost.csv')
        self.straw_cost         .to_csv('F:\\COWS\\data\\feed_data\\straw_cost.csv')
        self.total_feedcost     .to_csv('F:\\COWS\\data\\feed_data\\total_feedcost.csv')
        
        
        self.group_a_daily_feed_costpercow  .to_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\group_a_daily_feed_costpercow.csv')
        self.group_b_daily_feed_costpercow  .to_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\group_b_daily_feed_costpercow.csv')
        self.dry_daily_feed_costpercow      .to_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\dry_daily_feed_costpercow.csv')
        self.latest_daily_feed_costpercow   .to_csv('F:\\COWS\\data\\feed_data\\cost_per_cow\\latest_daily_feed_costpercow.csv')
        
        self.monthly_feedcost   .to_csv('F:\\COWS\\data\\feed_data\\feed_monthly_weekly\\monthly_feedcost.csv')
        self.weekly_feedcost    .to_csv('F:\\COWS\\data\\feed_data\\feed_monthly_weekly\\weekly_feedcost.csv')
        
        
        
        

def create_feed_cost(self, feed_type)    :
    
    base_path = 'F:/COWS/data/feed_data/feed_csv'
    self.price_seq_path = os.path.join(base_path, f'{feed_type}_price_seq.csv')
    self.daily_amt_path = os.path.join(base_path, f'{feed_type}_daily_amt.csv')
        
    price_seq   = pd.read_csv(self.price_seq_path,  header=0, index_col=0, parse_dates=['datex'])  
    daily_amt   = pd.read_csv(self.daily_amt_path,  header=0, index_col=0, parse_dates=['datex'])  
    
    
                        # create price sequence on new index 'date_range'
    price_seq = price_seq.reindex(self.date_range, method='ffill')
    price_seq.index.name = 'datex'
                        # same thing for daily amt
    daily_amt = daily_amt.reindex(self.date_range, method='ffill')
    daily_amt.index.name = 'datex'

    p1 = pd.merge(daily_amt, price_seq, left_index=True, right_index=True) 
    
                        # status brings count for each group each day
    p  = p1.merge(self.status.allcows,  left_index=True, right_index=True) 
        
    p['group_a_costpercow'] = p['group_a_kg']    * p['unit_price']
    p['group_b_costpercow'] = p['group_b_kg']    * p['unit_price']
    p['dry_costpercow']     = p['dry_kg']       * p['unit_price']      
    
                        # use in inventory calc
    p['group_a_kgperday']   = p['group_a_kg'] * p['group_a_count']
    p['group_b_kgperday']   = p['group_b_kg'] * p['group_b_count'] 
    p['dry_kgperday']       = p['dry_kg']     * p['dry_count']
    
    p['total kgperday']     = p['group_a_kgperday'] + p['group_b_kgperday'] + p['dry_kgperday']
        
    
    p['group_a dailycost']  = p['group_a_kg']    * p['unit_price'] * p['group_a_count']
    p['group_b dailycost']  = p['group_b_kg']    * p['unit_price'] * p['group_b_count']
    p['dry daily cost']     = p['dry_kg']        * p['unit_price'] * p['dry_count']
    
    p['total daily cost']   = p['group_a dailycost'] + p['group_b dailycost'] + p['dry daily cost']
    
    varname = f"{feed_type}_cost" 
    varname = pd.DataFrame(p)
        
    return varname
