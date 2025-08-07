'''run_insem_status_IU.py'''

from MilkBasics                             import MilkBasics
from CreateStartDate                        import DateRange
from status_functions.WetDry                import WetDry

from status_functions.statusData2           import StatusData2
from status_functions.statusData            import StatusData
from status_functions.status_groups         import statusGroups

from milk_functions.lactations.lactation_basics import Lactation_basics

from insem_functions.check_laststop         import CheckLastStop
from insem_functions.insem_ultra_basics     import InsemUltraBasics
from insem_functions.Insem_ultra_data       import InsemUltraData
from insem_functions.Ipiv                   import Ipiv
from insem_functions.I_U_merge              import I_U_merge

from feed_functions.feedcost_basics         import Feedcost_basics




class Main():
    def __init__ (self):
        self.milk_basics        = MilkBasics()
        self.date_range         = DateRange()
        self.insem_ultra_basics = InsemUltraBasics(milk_basics=self.milk_basics)
        self.status_data2       = StatusData2(date_range=self.date_range, insem_ultra_basics=self.insem_ultra_basics, milk_basics=self.milk_basics)
        self.status_data        = StatusData(date_range=self.date_range, milk_basics=self.milk_basics)
        self.wet_dry            = WetDry(milk_basics=self.milk_basics, date_range=self.date_range)
        self.status_groups      = statusGroups(date_range=self.date_range, status_data=self.status_data, wet_dry=self.wet_dry, milk_basics=self.milk_basics, insem_ultra_basics=self.insem_ultra_basics)
        self.lactation_basics   = Lactation_basics()
        self.insem_ultra_data   = InsemUltraData(insem_ultra_basics=self.insem_ultra_basics, status_data2=self.status_data2, milk_basics=self.milk_basics)
        self.ipiv               = Ipiv(insem_ultra_basics=self.insem_ultra_basics, insem_ultra_data=self.insem_ultra_data, milk_basics=self.milk_basics, status_data2=self.status_data2)
        self.i_u_merge          = I_U_merge(milk_basics=self.milk_basics)
        self.check_last_stop    = CheckLastStop(milk_basics=self.milk_basics,status_data2=self.status_data2, insem_ultra_basics=self.insem_ultra_basics, insem_ultra_data=self.insem_ultra_data)
        self.feed_cost_basics   = Feedcost_basics(milk_basics=self.milk_basics,status_data=self.status_data,date_range=self.date_range)
        
        
if __name__ == "__main__":
    Main()
