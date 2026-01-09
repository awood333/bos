'''finance_functions\\PL\\net_revenue_daily.py'''
import inspect
import pandas as pd
from container import get_dependency
from date_range import DateRange


class NetRevenue:
    def __init__(self):
        print(f"NetRevenue instantiated by: {inspect.stack()[1].filename}")
        self.DR = None
        self.startdate = None
        self.MI = None
        self.sahagon = None
        self.TF = None
        self.feedcost1 = None
        self.net_revenue_daily = None
        self.net_revenue_daily_last = None
        self.feedcost_daily = None
        self.test_daily_net = None
        self.net_revenue_monthly = None

    def load_and_process(self):
        self.DR = get_dependency('date_range')
        self.startdate = self.DR.start_date()
        self.MI = get_dependency('milk_income')
        self.sahagon = get_dependency('sahagon')
        self.TF = get_dependency('feedcost_total')

        self.feedcost1 = self.TF.feedcost
        self.feedcost1.index = pd.to_datetime(self.feedcost1.index, errors='coerce')

        [self.net_revenue_daily,
         self.net_revenue_daily_last,
         self.feedcost_daily] = self.create_net_revenue()

        # self.test_daily_net = self.create_test_daily_net()
        self.net_revenue_monthly = self.create_monthly_net()
        self.write_to_csv()
        
    
    def create_net_revenue(self):
        
        income1 = self.MI.income_daily
        income2 = income1.loc[self.startdate:,:]
        income3 = income2['avg gross'].copy()
        income3.index = pd.to_datetime(income3.index, errors='coerce')
        
        
        feedcost2 = self.feedcost1.loc[self.startdate:,:]
        feedcost3 = feedcost2['total feedcost'].copy()
        feedcost3.index = pd.to_datetime(feedcost3.index, errors='coerce')
        
        netrev1 = pd.concat((income3,feedcost3), axis=1)
        netrev1['net revenue'] = netrev1['avg gross'] - netrev1['total feedcost']
        
        self.net_revenue_daily = netrev1
        self.net_revenue_daily_last = self.net_revenue_daily.iloc[-5:, :]
        self.feedcost_daily = feedcost3
        
        return self.net_revenue_daily, self.net_revenue_daily_last, self.feedcost_daily
    
    def create_monthly_net(self):
        nrm1 = self.net_revenue_daily
        nrm1['year'] = nrm1.index.year
        nrm1['month'] = nrm1.index.month
                 
        self.net_revenue_monthly = nrm1.groupby(['year','month']).sum()
        return self.net_revenue_monthly

 
    def write_to_csv(self):
        
        # self.test_daily_net         .to_csv('F:\\COWS\\data\\PL_data\\test_daily_net.csv')
        self.net_revenue_daily_last .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_daily_last.csv')
        self.net_revenue_daily      .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_daily.csv')
        self.net_revenue_monthly    .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_monthly.csv')
        
        
        
        
if __name__ == "__main__":
    obj=NetRevenue()            
    obj.load_and_process()     