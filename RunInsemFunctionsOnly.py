'''RunInsemUltra.py'''

from insem_functions.I_U_merge import I_U_merge
from insem_functions.Insem_ultra_data import InsemUltraData
from insem_functions.insem_ultra_basics import InsemUltraBasics
from insem_functions.insem_data_backup import InsemDataBackup


class RunInsemFunctionsOnly:
    def __init__(self):
        
        InsemUltraBasics()    
        I_U_merge()
        InsemUltraData()
        InsemDataBackup()
                
     
       
if __name__ == "__main__":
    RunInsemFunctionsOnly()
