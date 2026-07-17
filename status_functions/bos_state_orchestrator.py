'''
status_functions.bos_state_orchestrator
'''

import inspect
from container import get_dependency, container

class BosStateOrchestrator:
    """
    Single source of truth for facts every downstream module needs:
    alive/dead/pregnant status, canonical wy_id list, as-of date.
    Consumed by: WetDry, ModelGroups, NetRevenue, PlotNetRevenueModel.
    """
    def __init__(self):
        
        print(f"BosStateOrchestrator instantiated by: {inspect.stack()[1].filename}")

        self.SD     = None
        self.MB     = None
        self.IUD    = None
        self.alive_ids      = None
        self.pregnant_ids   = None
        self.as_of_date     = None

    def load(self):
        self.SD  = get_dependency('status_data')
        self.MB  = get_dependency('milk_basics')
        self.IUD = get_dependency('insem_ultra_data')
        self.process()

    def process(self):
        
    #current - latest - data
        #most recent status_col
        status_last     = self.SD.status_col.iloc[:, 0] 
        #date of most recent status_col
        self.as_of_date = self.SD.status_col.columns[0] if hasattr(self.SD.status_col, 'columns') else None
        #alive_ids includes heifers, milking and dry
        self.alive_ids  = status_last[~status_last.isin(['gone', 'nby'])].index
        #pregnant now
        preg1 = self.IUD.all_preg.reset_index(drop=True)
        self.preg  = preg1[['wy_id','status','expected bdate']]
        self.pregnant_ids = set(self.preg['wy_id'])
    
    # historical data
        # self.wetdrydays = self.
        
        
if __name__=='__main__':
    obj = BosStateOrchestrator()
    obj.load()
    container.show_dependency_graph()
            