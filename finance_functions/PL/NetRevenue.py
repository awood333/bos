'''finance_functions\\PL\\net_revenue_daily.py'''

import pandas as pd

from CreateStartDate import DateRange
from finance_functions.income.MilkIncome import MilkIncome
from milk_functions.sahagon import sahagon
from feed_functions.feedcost_total import Feedcost_total
from feed_functions.heifers.heifer_cost_model import HeiferCostModel


class NetRevenue:
    def __init__ (self):
        
        DR = DateRange()
        self.startdate = DR.start_date()
        
        self.MI = MilkIncome()
        self.SG = sahagon()
        TF = Feedcost_total()
        HFC = HeiferCostModel()
        
        
        self.feedcost1 = TF.feedcost
        self.feedcost1.index = pd.to_datetime(self.feedcost1.index, errors='coerce')
        
        self.heifer_monthly_cost = HFC.heifer_feedcost_monthly.loc[:,'heifer_cost'].copy()

        [self.net_revenue_daily,
         self.net_revenue_daily_last,
         self.feedcost_daily]           = self.create_net_revenue()
        
        self.test_daily_net             = self.create_test_daily_net()
        
        self.net_revenue_monthly    = self.create_monthly_net()
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
        netrev1['revenue'] = netrev1['avg gross'] - netrev1['total feedcost']
        
        self.net_revenue_daily = netrev1
        self.net_revenue_daily_last = self.net_revenue_daily.iloc[-5:, :]
        self.feedcost_daily = feedcost3
        
        return self.net_revenue_daily, self.net_revenue_daily_last, self.feedcost_daily
    
    def create_test_daily_net(self):
        
        liters1 = self.SG.dm_daily
        liters1['income_22'] = liters1['liters'] * 22
        
        fc1= self.feedcost_daily
        df1= pd.concat([liters1, fc1], axis=1)
        df1['net_revenue_test'] = df1['income_22'] - df1['total feedcost']
        
        self.test_daily_net = df1
        
        return self.test_daily_net
        
    
    
    def create_monthly_net(self):
    
        nrm1 = self.net_revenue_daily
        nrm1['year'] = nrm1.index.year
        nrm1['month'] = nrm1.index.month
        # nrm1['days'] = nrm1.index.daysinmonth
        
        net_revenue_monthly1 = nrm1.groupby(['year','month']).sum()
        net_revenue_monthly2 = pd.concat([net_revenue_monthly1, self.heifer_monthly_cost], axis=1)
        net_revenue_monthly2['net revenue'] = net_revenue_monthly2['revenue'] - net_revenue_monthly2['heifer_cost']
        self.net_revenue_monthly = net_revenue_monthly2

        return self.net_revenue_monthly

 
    def write_to_csv(self):
        
        self.test_daily_net         .to_csv('F:\\COWS\\data\\PL_data\\test_daily_net.csv')
        self.net_revenue_daily_last .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_daily_last.csv')
        self.net_revenue_daily      .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_daily.csv')
        self.net_revenue_monthly    .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_monthly.csv')
        
        
        
        
if __name__ == "__main__":
    NetRevenue()            