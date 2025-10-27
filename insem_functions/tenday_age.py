import inspect
import pandas as pd 

from container import get_dependency
from persistent_container_service import ContainerClient

class TendayMilkingDays:
    def __init__(self):
        print(f"TendayMilkingDays instantiated by: {inspect.stack()[1].filename}")
        self.IUD = None
        self.MA = None
        self.days = None
        self.preg = None
        self.td2 = None

    def load_and_process(self):
        client = ContainerClient()
        self.IUD = client.get_dependency('insem_ultra_data')
        self.MA = client.get_dependency('milk_aggregates')
        self.days = self.IUD.allx.loc[:, ['WY_id', 'days milking']]
        self.preg = self.IUD.allx.loc[:, ['WY_id', 'u_read', 'expected bdate']]
        self.td2 = self.tenday_days()

    def tenday_days(self):
        td = self.MA.tenday.reset_index()
        self.td2 = pd.merge(td, self.preg, on='WY_id', how='left')
        self.td2.to_csv('F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday_days.csv')
        return self.td2
        
if __name__ ==     "__main__"    :
    obj = TendayMilkingDays()
    obj.load_and_process()      
