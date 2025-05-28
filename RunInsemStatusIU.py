'''run_insem_status_IU.py'''

from status_functions.statusData2            import StatusData2
from status_functions.statusData             import StatusData
from status_functions.status_groups          import statusGroups

from milk_functions.lactations.lactation_basics import Lactation_basics

from insem_functions.check_laststop         import CheckLastStop
from insem_functions.insem_ultra_basics     import InsemUltraBasics
from insem_functions.Insem_ultra_data       import InsemUltraData
from insem_functions.Ipiv                   import Ipiv
from insem_functions.I_U_merge              import I_U_merge

from feed_functions.feedcost_basics         import Feedcost_basics

# from finance_functions.PL.NetRevenue          import NetRevenue


class Main():
    def __init__ (self):

        self.statusData2        = StatusData2()
        self.statusData         = StatusData()
        self.statusGroups       = statusGroups()
        self.lactation_basics   = Lactation_basics()
        
        self.insem_ultra_data   = InsemUltraData()
        self.insem_ultra_basics = InsemUltraBasics()
        self.ipiv               = Ipiv()
        
        self.i_u_merge          = I_U_merge()
        self.check_last_stop    = CheckLastStop()
        self.feed_cost_basics   = Feedcost_basics()
        
        # self.net_revenue        = NetRevenue()


if __name__ == "__main__":
    Main()
