import os
import shutil
import pandas as pd

# Generate a timestamp for the backup
tdy = pd.Timestamp('now').strftime('%Y-%m-%d_%H-%M-%S')
from pathlib import Path

class FeedDataBackup:
    def __init__(self):
        # Use config_path nomenclature for cross-platform compatibility
        self.source_folder = Path.home() / "cows_data" / "feed_data"
        # Compose backup folder path using Path and config_path
        backup_root = Path.home() / 'cows_data' / 'data_backup' / 'feed_backup'
        self.backup_folder = backup_root / f'feed_backup_{tdy}'
        self.copy_folder_with_timestamp()

    def copy_folder_with_timestamp(self):
        # Copy the entire folder to the backup location with a timestamp
        if not self.backup_folder.exists():
            shutil.copytree(self.source_folder, self.backup_folder)
            print(f"Backup completed: {self.backup_folder}")
        else:
            print(f"Backup folder already exists: {self.backup_folder}")

if __name__ == "__main__":
    obj = FeedDataBackup()
    # obj.load_and_process()    