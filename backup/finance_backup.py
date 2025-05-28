
import pandas as pd

# Generate a timestamp for the backup
tdy = pd.Timestamp('now').strftime('%Y-%m-%d_%H-%M-%S')

class FinanceDataBackup:
    def __init__(self):

        self.write_to_csv()


        
    def write_to_csv(self):
        bkkbank = pd.read_csv('F:\\COWS\\data\\finance\\BKKbank\\BKKBankFarmAccount.csv')
        depr    = pd.read_csv('F:\\COWS\\data\\finance\\capex\\depreciation_schedule\\depreciation_schedule.csv')
        
        bkkbank .to_csv(f'F:\\COWS\\data\\finance\\BKKbank\\backup\\BKKBankFarmAccount_{tdy}.csv')
        bkkbank .to_csv(f'E:\\Cows\\data_backup\\finance_backup\\farm_account\\BKKBankFarmAccount_{tdy}.csv')
        depr    .to_csv(f'E:\\Cows\\data_backup\\finance_backup\\misc_finance_backup\\depreciation_schedule_{tdy}.csv')


if __name__ == "__main__":
    FinanceDataBackup()