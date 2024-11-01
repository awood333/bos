# RemoteFilesaveUtils.py

import subprocess
from concurrent.futures import ThreadPoolExecutor
import os

class RemoteFilesaveUtils:
    
    @staticmethod
    def create_remote_directory(remote_path, rclone_path='rclone'):
        try: 
            result = subprocess.run(
                [rclone_path, "mkdir", remote_path],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"Successfully created remote directory {remote_path}")
            # print(f"rclone output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create remote directory {remote_path}")
            print(f"rclone error: {e.stderr}")

    @staticmethod
    def copy_to_gdrive(local_path, remote_path, rclone_path='rclone', extra_flags=None):
        try:
            command = [rclone_path, "copy", local_path, remote_path]
            if extra_flags:
                command.extend(extra_flags.split())
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True
            )
            print(f"Successfully copied {local_path} to {remote_path}")
            print(f"rclone output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to copy {local_path} to {remote_path}")
            print(f"rclone error: {e.stderr}")