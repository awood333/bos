'''Run_insem_status_IU.py'''

from insem_functions.insem_ultra_basics import InsemUltraBasics
from insem_functions.insem_ultra_data   import InsemUltraData
from insem_functions.check_laststop     import CheckLastStop
from insem_functions.I_U_merge          import I_U_merge
from insem_functions.next_ultra_check   import NextUltraCheck

from status_functions.wet_dry           import WetDry
from status_functions.status_data       import status_data

from groups_and_tests.wet_dry_groups    import WetDryGroups


class RunInsemStatusIU:
    def __init__(self):

        self.insem_ultra_basics = InsemUltraBasics()
        self.insem_ultra_data   = InsemUltraData()
        self.check_last_stop    = CheckLastStop()
        self.i_u_merge          = I_U_merge()

        self.next_ultra_check   = NextUltraCheck()

        self.wet_dry            = WetDry()
        self.status_data        = status_data()
        self.wet_dry_groups     = WetDryGroups()



        self.insem_ultra_basics.load_and_process()
        self.insem_ultra_data.load_and_process()
        self.check_last_stop.load_and_process()
        self.i_u_merge.load_and_process()
        self.next_ultra_check.load_and_process()
        self.wet_dry.load_and_process()
        self.status_data.load_and_process()

        self.wet_dry_groups.load_and_process()

if __name__ == "__main__":
    obj = RunInsemStatusIU()
  