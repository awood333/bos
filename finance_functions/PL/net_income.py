''' finance_functions/PL/net_income.py '''
import inspect
import pandas as pd
from pathlib import Path
from container import get_dependency
from sql_db_related.neon_connect import get_engine

class NetIncome():
    def __init__(self):
        print(f"NetIncome instantiated by: {inspect.stack()[1].filename}")
        self.engine = get_engine()
        
    def load(self):
  
        self.NR = get_dependency('net_revenue')
        self.FB = get_dependency('finance_basics')

        self.process()
        
    def process(self):
        
        self.net_revenue      = self.NR.net_revenue_monthly
        self.total_cost_xfeed = self.FB.total_cost_xfeed
        self.total_cost_xfeed.index = pd.to_datetime(self.total_cost_xfeed.index).to_period('M')
        self.total_cost_xfeed.index.name = 'datex'

        with self.engine.connect() as conn:
            self.cost_xfeed_pivot= pd.read_sql_table('cost_x_feed_formatted', conn)
            
        self.non_feed_cost_df = self.FB.non_feed_cost_df
            
        
        #methods
        self.net_income = self.create_net_income()
        self.write_to_csv()
        
    def create_net_income(self):
        
        nr = self.net_revenue
     
        cost = self.total_cost_xfeed
               
        cost = cost.reset_index()
                 
        nr2 = pd.merge(nr, cost, how='outer', on='datex')
        nr2['net income'] = nr2['net_revenue'] - nr2['total xfeed cost']
        nr2 = nr2.rename(columns={'cost': 'feed cost'})
        
        nr2['liters shortfall'] = (nr2['net income'] / 22)/30
        nr2['liters for bkeven'] = -nr2['liters shortfall'] + nr2['avg liters']
        
        self.net_income = nr2
        
        
        return self.net_income
    
    def write_to_csv(self):
        output_dir = Path("/home/alanw/Documents/vsCode_output/finance")
        output_dir.mkdir(parents=True, exist_ok=True)
        self.net_income.to_csv(output_dir / "net_income.csv")   
        
        
if __name__ == "__main__":
    obj= NetIncome()
    obj.load()