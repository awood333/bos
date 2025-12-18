'''finance\\milk_income.py'''
import inspect
import pandas as pd

from container import get_dependency

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class MilkIncome:
    def __init__(self):
        print(f"MilkIncome instantiated by: {inspect.stack()[1].filename}")
        self.FCB = None
        self.SD = None
        self.DR = None
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
        self.SD  = get_dependency('status_data')
        self.DR  = get_dependency('date_range')
        self.sahagon = get_dependency('sahagon')
        self.sahagon_liters = self.sahagon.dm_daily

        self.income_data = self.DataLoader()
        self.income = self.calcMilkIncome()
        [self.income_daily, 
         self.income_monthly, 
         self.income_daily_last,
         self.sahagon_income_daily] = self.create_reindexed_daily_monthly()
        self.write_to_csv()
        
    def DataLoader(self):  

        income1 = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\data\\milk_income_data.csv')
        income1['datex'] = pd.to_datetime(income1['datex'] )
        income1.set_index('datex', inplace=True)
        
        self.income_data = income1

        return self.income_data
    
    def calcMilkIncome(self):

        income3 = self.income_data
        
        income3['avg liters']       = income3['liters']     / income3['day']
        income3['avg gross']        = income3['gross_baht'] / income3['day']
        income3['avg tax']          = income3['tax']        / income3['day']
        income3['avg other cost']   = income3['other']      / income3['day']
        income3['avg dtoc']         = income3['avg other cost'] + income3['avg tax'] 
        
        income4 = income3.drop(columns={ 'liters','gross_baht','tax','other' })
        self.income = income4
        
        return self.income
    
    def create_reindexed_daily_monthly(self):

        rng_daily           = self.DR.date_range_daily
        
        price               = self.income['base']
        price_daily         = price.reindex(rng_daily, method='bfill').ffill()
        price_daily.index   = pd.to_datetime(price_daily.index, errors='coerce')
        
        sahagon_liters1     = self.sahagon_liters.reindex(rng_daily)
        self.sahagon_income_daily = sahagon_liters1 * price_daily
        
        income_daily        = self.income.reindex(rng_daily, method='bfill').ffill()
        income_daily.index  = pd.to_datetime(income_daily.index, errors='coerce')
        income_daily2       = income_daily

        self.income_daily   = income_daily2.drop( columns = ['avg tax', 'avg other cost'])
         
        
        self.income_daily_last = self.income_daily.iloc[-3:,:]
        
        income_monthly1     = income_daily2.groupby(['year','month']).agg(
            {
                'avg liters' : 'mean',
                'avg gross'  : 'sum',
                'avg dtoc'    : 'sum'
            }
        )
        
        income_monthly2 = income_monthly1.rename(columns={'avg liters' : 'liters', 'avg gross' : 'gross', 'avg dtoc' : 'dtoc'})
        
        self.income_monthly = income_monthly2.loc[2024:,:]

        return self.income_daily, self.income_monthly, self.income_daily_last, self.sahagon_income_daily
    
    
    # BACKUP
    def write_to_csv(self):
        self.income     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_.csv')
        self.income     .to_csv('E:\\COWS\\data_backup\\milk_income_backup\\milk_income_'+tdy+'.csv')

        self.income_daily       .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_daily.csv')
        self.income_daily_last  .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_daily_last.csv')
        self.income_monthly     .to_csv('F:\\COWS\\data\\PL_data\\milk_income\\output\\milk_income_monthly.csv')
        

if __name__ == '__main__':
    obj=MilkIncome()
    obj.load_and_process()     
