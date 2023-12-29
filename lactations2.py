import lactations as lac
import pandas as pd
import numpy as np
lb = pd.read_csv('F:\\COWS\\data\\csv_files\\live_births.csv',header=0,parse_dates=['b_date'])
bd = pd.read_csv('F:\\COWS\\data\\csv_files\\birth_death.csv',header=0,parse_dates=['birth_date','death_date'])

cutoffdate = pd.to_datetime('9/1/2022',format='%m/%d/%Y')

newcows1 = (lb.loc[(
    lb['calf#'] == 1
    & (lb['b_date'] > cutoffdate)
    )])
newcowlist = list(newcows1['WY_id'])
# 

l1 = lac.lacx1.transpose()
milking1 = pd.DataFrame(l1)
newcows2 = milking1.loc[:,newcowlist]
newcows2T = newcows2.T.sort_index()
newcows3 = newcows2T.T
# 
wk1 = newcows3.merge(lac.step7,left_index = True,right_index = True)
wk2 = wk1.loc[:307,:].copy()                                #307 is 44 weeks
wk2.replace(0,np.nan,inplace=True)
# 
wk_L1 = wk2.groupby([666],as_index = True).mean()
wk_L1.drop(['n1','step7'],axis = 1,inplace = True)
wk_L1.index.name='week#'
wk_L1a = wk_L1.T
# 
wk_L1a.reset_index(drop=False,inplace=True)
wk_L1a.rename(columns={'index':'WY_id'},inplace=True)
# 
wk_L1b = wk_L1a.merge(lac.bd[['WY_id','death_date']],left_on='WY_id',right_on='WY_id')
dd = wk_L1b.pop('death_date')
wk_L1b.insert(1,'death date',dd)
wk_L1b.to_csv('F:\\COWS\\data\\milk_data\\lactations\\newcows.csv')

