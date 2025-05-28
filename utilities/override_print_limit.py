import pandas as pd


class override_print_limits:
    def __init__(self):
        self.override()
    
    def override(self):
                
        # Set pandas display options to show all rows and columns
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        