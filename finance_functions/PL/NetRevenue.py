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
        self.feedcost_weekly = None
        self.feedcost_monthly  = None
        self.income_weekly = None
        self.income_monthly = None
        self.net_revenue_weekly  = None
        self.net_revenue_monthly = None

    def load(self):
        self.DR   = get_dependency('date_range')
        self.MI   = get_dependency('milk_income')
        self.FCBD = get_dependency('feedcost_by_group_by_day')
        self.process()

    def process(self):
        self.startdate  = self.DR.startdate        
        self.feedcost_weekly   = self.FCBD.feedcost_by_group_by_week_df
        self.feedcost_monthly   = self.FCBD.feedcost_by_group_by_month_df

        self.income_weekly  = self.MI.income_weekly.copy()
        self.income_monthly  = self.MI.income_monthly.copy()
              
            
        #methhods

        self.net_revenue_weekly  = self.create_net_revenue_weekly()
        self.net_revenue_monthly = self.create_net_revenue_monthly()
        
    
    def create_net_revenue_weekly(self):
        income1 = self.income_weekly
        cost1 = self.feedcost_weekly

        # explicit alignment guard, same reasoning as model_groups.py:
        # equal shape doesn't guarantee equal index/columns
        income_a, cost_a = income1.align(cost1, join='inner')
        if income_a.shape != income1.shape or cost_a.shape != cost1.shape:
            print(f"WARNING: net_revenue_weekly alignment dropped cells — "
                f"income {income1.shape} -> {income_a.shape}, "
                f"cost {cost1.shape} -> {cost_a.shape}")

        net_revenue = income_a.sub(cost_a, fill_value=0)
        self.net_revenue_weekly = net_revenue
        return self.net_revenue_weekly



    
    def create_net_revenue_monthly(self):
        income1 = self.income_monthly
        cost1 = self.feedcost_monthly

        income_a, cost_a = income1.align(cost1, join='inner')
        if income_a.shape != income1.shape or cost_a.shape != cost1.shape:
            print(f"WARNING: net_revenue_monthly alignment dropped cells — "
                f"income {income1.shape} -> {income_a.shape}, "
                f"cost {cost1.shape} -> {cost_a.shape}")

        net_revenue = income_a.sub(cost_a, fill_value=0)
        self.net_revenue_monthly = net_revenue
        return self.net_revenue_monthly

 
if __name__ == "__main__":
    obj=NetRevenue()            
    obj.load()     