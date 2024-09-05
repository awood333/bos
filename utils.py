'''utils.py'''

import subprocess
from concurrent.futures import ThreadPoolExecutor
import os

def write_csv(data, path):
    # Implement the logic to write data to CSV
    pass

def copy_to_gdrive(local_path, remote_path, rclone_path='rclone'):
    subprocess.run(
        [rclone_path, "copy", local_path, remote_path],
        check=True,
        capture_output=True,
        text=True
    )
    print(f"Successfully copied {local_path} to {remote_path}")

def process_files(paths, data_provider, local_base_path, remote_base_path, rclone_path='rclone'):
    
    
    #writes locally
    with ThreadPoolExecutor() as executor:
        futures = []
        for name, relative_path in paths:
            local_path = os.path.join(local_base_path, *relative_path.split('/'))
            data = data_provider(name)
            futures.append(executor.submit(write_csv, data, local_path))
        for future in futures:
            future.result()  # Wait for all threads to complete
            
    #writes to remote
    with ThreadPoolExecutor() as executor:
        futures = []
        for name, relative_path in paths:
            local_path = os.path.join(local_base_path, *relative_path.split('/'))
            remote_path = os.path.join(remote_base_path, *relative_path.split('/'))
            futures.append(executor.submit(copy_to_gdrive, local_path, remote_path, rclone_path))
        for future in futures:
            future.result()  # Wait for all threads to complete