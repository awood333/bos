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
        self.sahagon = get_dependency('sahagon')
        self.sahagon_liters = self.sahagon.dm_daily

        self.income_data = self.DataLoader()
        self.income = self.calcMilkIncome()
        
        self.income_monthly  = self.create_reindexed_daily_monthly()
        self.write_to_csv()
        
    def DataLoader(self):  

        old_liters1 = pd.read_csv(r"F:\COWS\data\milk_data\fullday\fullday.csv")

        old_liters1['datex'] = pd.to_datetime(old_liters1['datex'] )
        old_liters1.set_index('datex', inplace=True)

        #create merged series of milk liters from pre-CP
        #take the sum of all liters (ignore heldback - should ~make up from milkers biased readings)
        old_total_liters2 = old_liters1.sum(axis=1)
        sahagon_cutoff_date = '2025-09-20'
        # Convert self.startdate (Timestamp) to string for robust date slicing
        if isinstance(self.startdate, pd.Timestamp):
            start_date_str = self.startdate.strftime('%Y-%m-%d')
        else:
            start_date_str = str(self.startdate)
        old_liters_pre_cutoff = old_total_liters2.loc[start_date_str:sahagon_cutoff_date]

        new_liters1 = pd.read_excel(r"F:\COWS\data\milk_data\daily_milk\daily_milk.xlsx", sheet_name='stats', header=0)
        new_litersT = new_liters1.T.reset_index()
        # If the first row is not a date, drop it (e.g., contains 'sale total', 'heldback AM', etc.)
        try:
            pd.to_datetime(new_litersT.loc[0, 'index'])
        except Exception:
            new_litersT = new_litersT.iloc[1:].reset_index(drop=True)
        new_litersT['index'] = pd.to_datetime(new_litersT['index'])
        # Filter using pd.Timestamp for comparison
        new_liters2 = new_litersT[new_litersT['index'] > sahagon_cutoff_date]
        # Set 'index' as the DataFrame index for clean summing and concatenation
        new_liters2 = new_liters2.set_index('index')
        new_liters2.index.name = 'datex'
        # Only use the first column (0), which is the 'sale total' row from pre-transpose
        new_liters4 = new_liters2.iloc[:, 0]
        income_data1 = pd.concat([old_liters_pre_cutoff, new_liters4], axis=0)
        income_data1 = income_data1.reset_index()
        income_data1.columns = ['datex', 'liters']
        self.income_data = income_data1


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
    def write_to_csv(self):
        self.income     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_.csv')
        self.income     .to_csv('E:\\COWS\\data_backup\\milk_income_backup\\milk_income_'+tdy+'.csv')

        self.income     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_daily.csv')
        # self.income_daily_last  .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_daily_last.csv')
        self.income_monthly     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_monthly.csv')
        

if __name__ == '__main__':
    obj=MilkIncome()
    obj.load_and_process()     
