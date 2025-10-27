'''Run_insem_status_IU.py'''

from container import get_dependency



class RunInsemStatusIU:
    def __init__(self):
        self.milk_basics = get_dependency('milk_basics')
        self.date_range = get_dependency('date_range')
        self.lactation_basics = get_dependency('lactation_basics')
        
        self.wet_dry = get_dependency('wet_dry')
        self.status_data = get_dependency('status_data')
        self.status_data2 = get_dependency('status_data2')
        self.model_groups = get_dependency('model_groups')
        
        self.feedcost_basics = get_dependency('feedcost_basics')
        
        self.check_last_stop = get_dependency('check_last_stop')
        self.insem_ultra_basics = get_dependency('insem_ultra_basics')
        self.insem_ultra_data = get_dependency('insem_ultra_data')
        self.ipiv = get_dependency('ipiv')
        self.i_u_merge = get_dependency('i_u_merge')

if __name__ == "__main__":
    obj = RunInsemStatusIU()
    obj.load_and_process()    