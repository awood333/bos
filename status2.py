import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
#
f=pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')
milking= f.iloc[-3:,:-5].copy()


milking.set_index('datex',inplace=True)
rng1 = milking.columns.to_list()
rng = [x+1 for x in range(len(rng1))]
i = 1
j = 1

m4 = []
while i < len(milking):
    m3 = []
    for j in rng:
        # m1 =  milking.iloc[-j:,:].values
        m1 =  milking.iloc[-j:,:]   
        m1a = m1.T
        # label = m1a[1].name
        # m2 = [index for index , value in enumerate(m1.flatten()) if value>0]
        m2 = m1.loc[(m1[1] > 0     )       ]
        m3.append(m2)
    m4.append(m3)
    i += 1
# m1 =  milking.iloc[-1:,:]
m4