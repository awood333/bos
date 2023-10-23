import subprocess
import pandas as pd
from datetime import datetime
from status import main
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
script_names = [
                # 'D:/Git_repos/bos/insem_ultra.py',
                # 'D:/Git_repos/bos/rawmilkupdate.py',
                # 'D:/Git_repos/bos/milkaggregates.py',              
                    # 'D:/Git_repos/bos/status.py',
                    # 'D:/Git_repos/bos/status2.py',
                    # 'D:/Git_repos/bos/wet_dry.py'              
                ]
# 'D:/Git_repos/bos/status_prep.py'

for script in script_names:
    subprocess.run(['python', script])
    
    
    
# #   from status, 


startdate = '2023-6-1'
maxdate = dmt2.max()
stopdate    = maxdate
date_format = '%Y-%m-%d'

sd = main(startdate, stopdate, date_format)

sd['allcows']        .to_csv('F:\\COWS\\data\\status\\status_all.csv')
sd['dry_ids_df']     .to_csv('F:\\COWS\\data\\status\\dry_ids_df.csv')
sd['dry_count_df']   .to_csv('F:\\COWS\\data\\status\\dry_ids_df.csv')
sd['milkers_ids']    .to_csv('F:\\COWS\\data\\status\\milkers_ids_df.csv')    
sd['days_milking_df'].to_csv('F:\\COWS\\data\\status\\days_milking_df.csv')