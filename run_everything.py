import subprocess
import pandas as pd

dmAM_liters=pd.read_excel ('F:\\COWS\\data\\daily_milk.xlsm',     sheet_name='AM_liters', skiprows=2,index_col=0,header=0)
AM_liters=pd.read_csv   ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv',          index_col=0,header=0)
d1 = dmAM_liters.columns[-1]
d2 = AM_liters.columns[-1]
d1
 
# #  ['rawmilkupdate.py', 'milkaggregates.py']
# script_names = ['D:/Git_repos/bos/rawmilkupdate.py',
#                 'D:/Git_repos/bos/milkaggregates.py',
#                 # 'D:/Git_repos/bos/lactations.py',
#                 # 'D:/Git_repos/bos/status.py',
#                 # 'D:/Git_repos/bos/wet_dry.py'                
#                 ]
# # 'D:/Git_repos/bos/status_prep.py'
# #

# for script in script_names:
#     subprocess.run(['python', script])

