'''finance_functions\\PL\\net_revenue_daily.py'''
import inspect
import pandas as pd
from container import get_dependency

class NetRevenue:
    def __init__(self):
        print(f"NetRevenue instantiated by: {inspect.stack()[1].filename}")
        self.DR = None
        self.MI = None
        self.FCBD = None
        
        
        self.startdate = None        
        self.feedcost_by_group_by_week_df = None
        self.feedcost_by_group_by_month_df  = None
        self.income_weekly = None
        self.income_monthly = None
        self.net_revenue_weekly  = None
        self.net_revenue_monthly_all = None

    def load(self):
        self.DR   = get_dependency('date_range')
        self.MI   = get_dependency('milk_income')
        self.FCBD = get_dependency('feedcost_by_group_by_day')
        self.process()

    def process(self):
        self.startdate  = self.DR.startdate    
        self.feedcost_by_group_by_day_df    = self.FCBD.feedcost_by_group_by_day_df
        self.feedcost_by_group_by_week_df   = self.FCBD.feedcost_by_group_by_week_df
        self.feedcost_by_group_by_month_df  = self.FCBD.feedcost_by_group_by_month_df

        self.income_daily    = self.MI.income_daily.copy()
        self.income_weekly   = self.MI.income_weekly.copy()
        self.income_monthly  = self.MI.income_monthly.copy()
              
            
        #methhods
        self.net_revenue_daily      = self.create_net_revenue_daily()
        self.net_revenue_weekly     = self.create_net_revenue_weekly()
        self.net_revenue_monthly    = self.create_net_revenue_monthly()
        
        
    
    def create_net_revenue_daily(self):
        income1 = self.income_daily
        cost1 = self.feedcost_by_group_by_day_df

        # explicit alignment guard, same reasoning as model_groups.py:
        # equal shape doesn't guarantee equal index/columns
        income_1, cost_1 = income1.align(cost1, join='inner')
        if income_1.shape != income1.shape or cost_1.shape != cost1.shape:
            print(f"WARNING: net_revenue_daily alignment dropped cells — "
                f"income {income1.shape} -> {income_1.shape}, "
                f"cost {cost1.shape} -> {cost_1.shape}")

        net_revenue = income_1.sub(cost_1, fill_value=0)
        self.net_revenue_daily = net_revenue        
        return self.net_revenue_daily
    
    def create_net_revenue_weekly(self):
        income1 = self.income_weekly
        cost1 = self.feedcost_by_group_by_week_df

        # explicit alignment guard, same reasoning as model_groups.py:
        # equal shape doesn't guarantee equal index/columns
        income_1, cost_1 = income1.align(cost1, join='inner')
        if income_1.shape != income1.shape or cost_1.shape != cost1.shape:
            print(f"WARNING: net_revenue_weekly alignment dropped cells — "
                f"income {income1.shape} -> {income_1.shape}, "
                f"cost {cost1.shape} -> {cost_1.shape}")

        net_revenue = income_1.sub(cost_1, fill_value=0)
        self.net_revenue_weekly = net_revenue
        return self.net_revenue_weekly



    
    def create_net_revenue_monthly(self):
        income1 = self.income_monthly.copy()
        cost1   = pd.DataFrame(self.feedcost_by_group_by_month_df.sum(axis=1).rename('cost'))
        
        # format as monthly period: 2025-06 instead of 2025-06-30
        # this eliminates the prob of one df being 2026-06-01 and the other 2026-06-31
        income1.index = pd.to_datetime(income1.index).to_period('M')
        cost1.index   = pd.to_datetime(cost1.index)  .to_period('M')
        
        cost1 = cost1.groupby(level=0).sum()
        cost1 = cost1.reindex(income1.index)
        income_1, cost_1 = income1, cost1

        net_revenue = income_1['income'] - (cost_1['cost']) 
        self.net_revenue_monthly = pd.DataFrame({
            'avg liters':  income_1['avg liters'],
            'income':      income_1['income'],
            'cost':        cost_1['cost'],
            'net_revenue': net_revenue            
        })
        return self.net_revenue_monthly

 
if __name__ == "__main__":
    obj=NetRevenue()            
    obj.load()     