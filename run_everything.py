import subprocess

 
#  ['rawmilkupdate.py', 'milkaggregates.py']
script_names = ['D:/Git_repos/bos/rawmilkupdate.py',
                'D:/Git_repos/bos/milkaggregates.py',
                'D:/Git_repos/bos/lactations.py'
                'D:/Git_repos/bos/status.py'
                'D:/Git_repos/bos/wet_dry.py'                
                ]
# 'D:/Git_repos/bos/status_prep.py'
#

for script in script_names:
    subprocess.run(['python', script])

