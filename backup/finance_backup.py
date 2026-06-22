import pandas as pd


# Generate a timestamp for the backup
tdy = pd.Timestamp('now').strftime('%Y-%m-%d_%H-%M-%S')

class FinanceDataBackup:
    def __init__(self):

        self.write_to_csv()


        
    def write_to_csv(self):
        
        # read from the gdrive and copy to D:\backup
        
        bkkbank = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "finance" / "BKKbank" / "BKKBankFarmAccount.csv")
        depr    = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "finance" / "capex" / "depreciation_schedule" / "depreciation_schedule.csv")
        milk_inc= pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "finance" / "ASG_milk_income" / "milk_income_data.csv")
        
        bkkbank.to_csv(Path.home() / "cows_data" / "finance_data" / "BKKbank" / f"BKKBankFarmAccount_{tdy}.csv")
        depr.to_csv(Path.home() / "cows_data" / "finance_data" / "capex" / f"depreciation_schedule_{tdy}.csv")
        milk_inc.to_csv(Path.home() / "cows_data" / "finance_data" / "ASG_milk_income" / f"milk_income_{tdy}.csv")
        


if __name__ == "__main__":
    obj = FinanceDataBackup()
