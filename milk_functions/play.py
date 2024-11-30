import pandas as pd 
import numpy as np 

from milk_functions.WetDry import WetDry



class Play:
    def __init__(self):
        self.wd = milk_functions.WetDry()
        self.wet3 = self.play()
        self.play_dash_vars = self.get_dash_vars()

    def play(self):
        wet3 = wd.wet_dict
        return wet3

    

    def get_dash_vars(self):
        self.play_dash_vars = {name: value for name, value in vars(self).items()
               if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.play_dash_vars
    
if __name__ == '__main__':
    play = Play()
