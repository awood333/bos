'''run_insem_status_IU.py'''

from milk_functions.statusData2            import StatusData2
from milk_functions.statusData             import StatusData

from insem_functions.insem_ultra_basics     import InsemUltraBasics
from insem_functions.Insem_ultra_data       import InsemUltraData
from insem_functions.Ipiv                   import Ipiv

from insem_functions.I_U_merge              import I_U_merge
from milk_functions .check_laststop         import CheckLastStop
from feed_functions.feed_cost_basics        import FeedCostBasics


class Main():
    def __init__ (self):

        self.statusData2        = StatusData2()
        self.statusData         = StatusData()
        
        self.insem_ultra_data   = InsemUltraData()
        self.insem_ultra_basics = InsemUltraBasics()
        self.ipiv               = Ipiv()
        
        self.i_u_merge          = I_U_merge()
        self.check_last_stop    = CheckLastStop()
        self.feed_cost_basics   = FeedCostBasics()


if __name__ == "__main__":
    Main()
