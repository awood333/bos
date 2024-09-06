'''
RunMilkDailyUpdate.py
'''

import pandas as pd
import os
import subprocess
from RemoteFilesaveUtils import RemoteFilesaveUtils
from milkaggregates import MilkAggregates

rfu = RemoteFilesaveUtils()
ma = MilkAggregates()

lbp = ma.local_base_path
rbp = ma.remote_base_path


def write_to_local():
    for df_name, subdir in (ma.paths):
        df = getattr(ma, df_name)
        local_path = f"{lbp}{subdir}"
        df.to_csv(local_path, index=True)

def write_to_remote(rclone_path='rclone'):
    
    for df_name, subdir in (ma.paths):
        df = getattr(ma, df_name)
        local_path = f"{lbp}{subdir}"
        remote_subdir = subdir.replace('\\', '/')
        remote_subdir_no_ext = os.path.splitext(remote_subdir)[0]
        remote_path = f"{rbp}/{remote_subdir_no_ext}"
        
        # rfu.create_remote_directory(os.path.dirname(remote_path), rclone_path)
        rfu.copy_to_gdrive( local_path, remote_path, rclone_path, extra_flags="--ignore-size")

write_to_local()  
write_to_remote()