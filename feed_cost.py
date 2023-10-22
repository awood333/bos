'''
feed_cost.py
'''
import pandas as pd
import numpy as np
import status

# allcows = pd.read_csv('F:\\COWS\\data\\status\\status_all.csv')

f1  = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv',
                  index_col=0,
                  header=0,
                  parse_dates=['datex'])

# date_format = '%m/%d/%Y'
# date_format = '%Y-%m-%d'

maxdate     = f1.index.max()  
# stopdate    = '2023-10-20' 
stopdate    = maxdate
startdate   = '2023-6-1'

date_range, date_range_cols                     = status.create_date_range(startdate, stopdate)
alive_mask, alive_count_df, gone_mask, gone_count_df,  nby_count_df,   ungone, allcows = status.create_alive_mask(startdate, stopdate)
live_ids_df                                     = status.create_alive_ids(alive_mask, startdate, stopdate)
milkers_mask, milkers_mask_df, milkers_count_df = status.create_milkers_mask(startdate, stopdate)
milkers_ids                                     = status.create_milkers_ids(milkers_mask, startdate, stopdate)
days_milking_df                                 = status.create_days(milkers_mask, startdate, stopdate)

group_a, group_a_count  = status.create_group_a(days_milking_df, startdate, stopdate)
group_b, group_b_count  = status.create_group_b(days_milking_df, startdate, stopdate)
group_a_ids_df          = status.create_group_a_ids(group_a, startdate, stopdate)
groupb_ids_df           = status.create_group_b_ids(group_b, startdate, stopdate)
dry_ids_df, dry_count_df= status.create_dry_ids(alive_mask, startdate, stopdate)
allcows                 = status.check_group_sums(allcows, group_a_count, group_b_count, milkers_count_df, dry_count_df)

f = f1.loc[date_range].copy()    #partition the milk dbase

drop_col_names = [
'feed_type_y',
'weight'    ,
'alive'     ,
'nby'       ,
'gone'      ,
'sum'       ,
'group_sum' ,
'milkers'   ,
'milk+dry'  
]

rename_mask = {'feed_type_x':'feed type', 'group_a_kg':'group_a kg',
               'group_b_kg':'group_b kg', 'dry_kg':'dry kg' ,
               'lot_sequence':'lot#', 'group_a_count':'group_a count',
               'group_b_count':'group_b count','dry_count':'dry count',
               'group_a_dailycost':'group_a daily cost', 
               'group_b_dailycost':'group_b daily cost',
               'dry_daily cost':'dry daily cost',
               'total_daily_cost':'total daily cost'
               }



def create_cassava_cost(date_range):
    
    price_seq = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\cassava_price_seq.csv',  header=0, index_col=0, parse_dates=['datex'])  
    daily_amt  = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\cassava_daily_amt.csv', header=0, index_col=0, parse_dates=['datex'])  
    
    price_seq = price_seq.reindex(date_range, method='ffill')
    price_seq.index.name = 'datex'
        
    daily_amt = daily_amt.reindex(date_range, method='ffill')
    daily_amt.index.name = 'datex'

    p1 = pd.merge(daily_amt, price_seq, left_index=True, right_index=True) 
    p  = p1.merge(status.allcows, left_index=True, right_index=True) 
    
    p['group_a dailycost']  = p['group_a_kg']    * p['unit_price'] * p['group_a_count']
    p['group_b dailycost']  = p['group_b_kg']    * p['unit_price'] * p['group_b_count']
    p['dry daily cost']    = p['dry_kg']           * p['unit_price'] * p['dry_count']
    p['total daily cost']   = p['group_a dailycost'] + p['group_b dailycost'] + p['dry daily cost']
    
    cassava_cost = pd.DataFrame(p)
    cassava_cost.drop(columns=drop_col_names, axis=1, inplace=True)
    cassava_cost.rename(columns=rename_mask, inplace=True)
    
    return cassava_cost
cassava_cost = create_cassava_cost(date_range)



def create_beans_cost(date_range):
    
    price_seq  = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\beans_price_seq.csv', header=0, index_col=0, parse_dates=['datex'])  
    daily_amt  = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\beans_daily_amt.csv', header=0, index_col=0, parse_dates=['datex'])  
    
    price_seq = price_seq.reindex(date_range, method='ffill')
    price_seq.index.name = 'datex'
        
    daily_amt = daily_amt.reindex(date_range, method='ffill')
    daily_amt.index.name = 'datex'

    p1 = pd.merge(daily_amt, price_seq, left_index=True, right_index=True) 
    p  = p1.merge(status.allcows, left_index=True, right_index=True) 
    
    p['group_a dailycost']  = p['group_a_kg']    * p['unit_price'] * p['group_a_count']
    p['group_b dailycost']  = p['group_b_kg']    * p['unit_price'] * p['group_b_count']
    p['dry daily cost']    = p['dry_kg']           * p['unit_price'] * p['dry_count']
    p['total daily cost']   = p['group_a dailycost'] + p['group_b dailycost'] + p['dry daily cost']
    
    bean_cost = pd.DataFrame(p)
    bean_cost.drop(columns=drop_col_names, axis=1, inplace=True)
    bean_cost.rename(columns=rename_mask, inplace=True)
    
    return bean_cost
beans_cost = create_beans_cost(date_range)




def create_corn_cost(date_range):
    
    price_seq = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\corn_price_seq.csv',  header=0, index_col=0, parse_dates=['datex'])  
    daily_amt  = pd.read_csv('F:\\COWS\\data\\feed_data\\feed_csv\\corn_daily_amt.csv', header=0, index_col=0, parse_dates=['datex'])  
    
    price_seq = price_seq.reindex(date_range, method='ffill')
    price_seq.index.name = 'datex'
        
    daily_amt = daily_amt.reindex(date_range, method='ffill')
    daily_amt.index.name = 'datex'

    p1 = pd.merge(daily_amt, price_seq, left_index=True, right_index=True) 
    p  = p1.merge(status.allcows, left_index=True, right_index=True) 
    
    p['group_a dailycost']  = p['group_a_kg']    * p['unit_price'] * p['group_a_count']
    p['group_b dailycost']  = p['group_b_kg']    * p['unit_price'] * p['group_b_count']
    p['dry daily cost']    = p['dry_kg']           * p['unit_price'] * p['dry_count']
    p['total daily cost']   = p['group_a dailycost'] + p['group_b dailycost'] + p['dry daily cost']
    
    corn_cost = pd.DataFrame(p)
    corn_cost.drop(columns=drop_col_names, axis=1, inplace=True)
    corn_cost.rename(columns=rename_mask, inplace=True)
    return corn_cost

corn_cost=create_corn_cost(date_range)








def create_total_feed_cost(date_range, corn_cost, cassava_cost, beans_cost):
    tfc = pd.DataFrame()
    tfc['beans']    = beans_cost['total daily cost']
    tfc['cassava']  = cassava_cost['total daily cost']
    tfc['corn']     = corn_cost['total daily cost']
    tfc['total feed cost'] = (beans_cost['total daily cost'] + cassava_cost['total daily cost'] + corn_cost['total daily cost'])
    
    total_feed_cost = pd.DataFrame(tfc)
    return total_feed_cost

total_feed_cost = create_total_feed_cost(date_range, corn_cost, cassava_cost, beans_cost)
# print(total_feed_cost.head(2))

def create_monthly_feedcost(total_feed_cost):
    tfc = total_feed_cost
    tfc['year'] = tfc.index.year
    tfc['month'] = tfc.index.month
    tfc_m = tfc.groupby(by=['year', 'month']).sum()

    total_monthly_feed_cost = tfc_m.reset_index()
    return total_monthly_feed_cost

monthly_feedcost = create_monthly_feedcost(total_feed_cost)
# print('total_monthly_feed_cost', monthly_feedcost)


# # write to csv
corn_cost.to_csv('F:\\COWS\\data\\feed_data\\corn_cost.csv')
cassava_cost.to_csv('F:\\COWS\\data\\feed_data\\cassava_cost.csv')

beans_cost.to_csv('F:\\COWS\\data\\feed_data\\beans_cost.csv')
monthly_feedcost.to_csv('F:\\COWS\\data\\feed_data\\monthly_feed_cost.csv')
