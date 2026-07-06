'''finance\\milk_income.py'''
import inspect
import pandas as pd

from container import get_dependency

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class MilkIncome:
    def __init__(self):
        print(f"MilkIncome instantiated by: {inspect.stack()[1].filename}")
        self.FCB = None
        self.DR = None
        self.startdate = None
        self.sahagon = None
        self.sahagon_liters = None
        self.income_data = None
        self.income = None
        self.income_daily = None
        self.income_monthly = None
        self.income_daily_last = None
        self.sahagon_income_daily = None

    def load_and_process(self):
        self.FCB = get_dependency('feedcost_basics')
        self.DR  = get_dependency('date_range')
        self.startdate = self.DR.start_date()

        self.income_data = self.DataLoader()
        self.income = self.calcMilkIncome()
        
        self.income_monthly  = self.create_reindexed_daily_monthly()
        
    def DataLoader(self):  

        MAB = get_dependency('milk_aggregates_basic')
        liters1 = MAB.fullday.copy()



        return self.income_data
    
    def calcMilkIncome(self):

        income3 = self.income_data
        # Calculate 'avg liters' as just 'liters' for each row (for later aggregation)
        income3['avg_liters'] = income3['liters']
        income3['baht'] = income3['liters'] * 22
        self.income = income3
        return self.income
    
    def create_reindexed_daily_monthly(self):

        income_monthly1 = self.income.copy()
        income_monthly2 = income_monthly1.set_index(income_monthly1['datex'], drop=True)
        income_monthly2['year'] = income_monthly2.index.year
        income_monthly2['month'] = income_monthly2.index.month
        income_monthly2 = income_monthly2.drop(columns=['datex'])

        # Aggregate 'baht' and 'liters' as sum, 'avg_liters' as mean
        income_monthly3 = income_monthly2.groupby(['year', 'month']).agg({
            'liters': 'sum',
            'avg_liters': 'mean',
            'baht': 'sum'
        })

        self.income_monthly = income_monthly3.loc[2025:,:]
        return self.income_monthly
    
    
    # BACKUP
    # def write_to_csv(self):
    #     from pathlib import Path
    #     milk_income_dir = Path.home() / "cows_data" / "finance_data" / "PL_data" / 'milk_income' / 'output'
    #     milk_income_dir.mkdir(parents=True, exist_ok=True)
    #     backup_dir = Path.home() / 'cows_data' / 'data_backup' / 'milk_income_backup'
    #     backup_dir.mkdir(parents=True, exist_ok=True)

    #     self.income.to_csv(milk_income_dir / 'milk_income_.csv')
    #     self.income.to_csv(backup_dir / f'milk_income_{tdy}.csv')
    #     self.income.to_csv(milk_income_dir / 'milk_income_daily.csv')
    #     # self.income_daily_last.to_csv(milk_income_dir / 'milk_income_daily_last.csv')
    #     self.income_monthly.to_csv(milk_income_dir / 'milk_income_monthly.csv')
        

if __name__ == '__main__':
    obj=MilkIncome()
    obj.load_and_process()     
