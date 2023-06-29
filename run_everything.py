import subprocess

 
#  ['rawmilkupdate.py', 'milkaggregates.py']
script_names = ['D:/Git_repos/bos/run_all/rawmilkupdate.py',
                'D:/Git_repos/bos/run_all/milkaggregates.py',
                'D:/Git_repos/bos/run_all/lactations.py'
                'D:/Git_repos/bos/run_all/status.py'
                'D:/Git_repos/bos/run_all/wet_dry.py'                
                ]
# 'D:/Git_repos/bos/run_all/status_prep.py'

for script in script_names:
    subprocess.run(['python', script])

