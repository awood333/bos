'''RunInsemUltra.py'''

from insem_functions.InsemUltraFunctions import InsemUltraFunctions
from insem_functions.I_U_merge import I_U_merge
from insem_functions.InsemUltraData import InsemUltraData
from insem_functions.InsemUltraBasics import InsemUltraBasics


class Main:
    def __init__(self):
        InsemUltraFunctions()
        InsemUltraBasics()
        I_U_merge()
        InsemUltraData()
     
       
if __name__ == "__main__":
    Main()
