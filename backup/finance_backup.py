import pandas as pd


# Generate a timestamp for the backup
tdy = pd.Timestamp('now').strftime('%Y-%m-%d_%H-%M-%S')

class FinanceDataBackup:
    def __init__(self):

        self.write_to_csv()


        
    def write_to_csv(self):
        
        # read from the gdrive and copy to D:\backup
        
        from config_path import GDRIVE_BKKBANK_DIR, GDRIVE_CAPEX_DIR, GDRIVE_ASG_MILK_INCOME_DIR
        
        bkkbank = pd.read_csv(GDRIVE_BKKBANK_DIR / "BKKBankFarmAccount.csv")
        depr    = pd.read_csv(GDRIVE_CAPEX_DIR / "depreciation_schedule.csv")
        milk_inc= pd.read_csv(GDRIVE_ASG_MILK_INCOME_DIR / "milk_income_data.csv")
        
        from config_path import LOCAL_BKKBANK, LOCAL_CAPEX, LOCAL_PROJECTS, LOCAL_ASG_MILK_INCOME
        
        bkkbank.to_csv(LOCAL_BKKBANK / f"BKKBankFarmAccount_{tdy}.csv")
        depr.to_csv(LOCAL_CAPEX / f"depreciation_schedule_{tdy}.csv")
        milk_inc.to_csv(LOCAL_ASG_MILK_INCOME / f"milk_income_{tdy}.csv")
        


if __name__ == "__main__":
    obj = FinanceDataBackup()
