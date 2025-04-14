import os
import shutil
import pandas as pd

# Generate a timestamp for the backup
tdy = pd.Timestamp('now').strftime('%Y-%m-%d_%H-%M-%S')

class FinanceDataBackup:
    def __init__(self):
        self.source_folder = 'F:\\COWS\\data\\finance'
        self.backup_folder = f'E:\\Cows\\data_backup\\finance backup\\finance_backup_{tdy}'
        self.copy_files_with_timestamp()
        self.write_to_csv()

    def copy_files_with_timestamp(self):
        # Create the backup folder if it doesn't exist
        if not os.path.exists(self.backup_folder):
            os.makedirs(self.backup_folder)

        # Walk through the source folder and copy files with a timestamp
        for root, _, files in os.walk(self.source_folder):
            relative_path = os.path.relpath(root, self.source_folder)
            target_folder = os.path.join(self.backup_folder, relative_path)
            os.makedirs(target_folder, exist_ok=True)

            for file in files:
                source_file = os.path.join(root, file)
                file_name, file_ext = os.path.splitext(file)
                target_file = os.path.join(target_folder, f"{file_name}_{tdy}{file_ext}")
                shutil.copy2(source_file, target_file)

        print(f"Backup completed: {self.backup_folder}")
        
    def write_to_csv(self):
        bkkbank = pd.read_excel('F:\\COWS\\data\\BKKBankFarmAccount.ods')
        bkkbank.to_csv(f'E:\\Cows\\data_backup\\finance backup\\farm acount\\BKKBankFarmAccount_{tdy}.csv')

if __name__ == "__main__":
    FinanceDataBackup()