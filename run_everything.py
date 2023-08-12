import subprocess
import pandas as pd
from datetime import datetime
# import sys
# print("sys exec  ",sys.executable)
# print("sys path",sys.path)
# 
# 
dmAM_liters=pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_liters', skiprows=2,index_col=0,header=0)
AM_liters=pd.read_csv   ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',          index_col=0,header=0)
dmtrans = dmAM_liters.T
AMtrans = AM_liters.T
# 
dmt2 = pd.to_datetime(dmtrans.index)
amt2 = pd.to_datetime(AMtrans.index)       
dmt2[-1] == amt2[-1], dmt2[-1],amt2[-1]
# 
d1 = dmAM_liters.columns[-1]
d2 = AM_liters.columns[-1]

#  ['rawmilkupdate.py', 'milkaggregates.py']
script_names = ['D:/Git_repos/bos/rawmilkupdate.py',
                'D:/Git_repos/bos/milkaggregates.py',
                # 'D:/Git_repos/bos/lactations.py',
                # 'D:/Git_repos/bos/status.py',
                # 'D:/Git_repos/bos/wet_dry.py'                
                ]
# 'D:/Git_repos/bos/status_prep.py'

for script in script_names:
    subprocess.run(['python', script])