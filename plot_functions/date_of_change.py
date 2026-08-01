'''plot_functions/date_of_change.py'''

import pandas as pd
class DateOfChange:
    def __init__(self):
        self.date_of_change1 = None 
        self.date_of_change2 = None

    def load_and_process(self):
        
        self.date_of_change1 = pd.to_datetime('2025-09-27').date()
        self.date_of_change2 = pd.to_datetime('2025-11-04').date()
        
        return self.date_of_change1, self.date_of_change2
    
if __name__ == "__main__":
    DateOfChange()
        
        