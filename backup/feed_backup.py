import os
import shutil
import pandas as pd

# Generate a timestamp for the backup
tdy = pd.Timestamp('now').strftime('%Y-%m-%d_%H-%M-%S')

class FeedDataBackup:
    def __init__(self):
        self.source_folder = 'F:\\COWS\\data\\feed_data'
        self.backup_folder = f'E:\\Cows\\data_backup\\feed_backup\\feed_backup_{tdy}'
        self.copy_folder_with_timestamp()

    def copy_folder_with_timestamp(self):
        # Copy the entire folder to the backup location with a timestamp
        if not os.path.exists(self.backup_folder):
            shutil.copytree(self.source_folder, self.backup_folder)
            print(f"Backup completed: {self.backup_folder}")
        else:
            print(f"Backup folder already exists: {self.backup_folder}")

if __name__ == "__main__":
    obj = FeedDataBackup()
    # obj.load_and_process()    