'''Lactations'''
import inspect
import pandas as pd
import numpy as np
from container import get_dependency, container



class LactationsLogStandard:
    def __init__(self):
        
        print(f"MilkAggregates instantiated by: {inspect.stack()[1].filename}")
        self.MB = get_dependency('milkbasics')
        self.LB = get_dependency('lactationbasics')
        self.L  = get_dependency('lactations')
        self.LTL= get_dependency('thislactation')
        self.LW = get_dependency('weeklylactations')  # Ensure proper instantiation

        self.m1_log_daily  = self.create_log_standard_daily()
        self.m1_log_weekly = self.create_log_standard_weekly()        



    def create_log_standard_daily(self):
        m1 = pd.read_csv("F:\\COWS\\data\\milk_data\\lactations\\daily\\milking.csv", index_col=0)
        self.m1_log_daily = np.log(m1)
        return self.m1_log_daily


    def create_log_standard_weekly(self):
        m1 = pd.read_csv("F:\\COWS\\data\\milk_data\\lactations\\weekly\\milking.csv", index_col=0)
        self.m1_log_weekly = np.log(m1)
        return self.m1_log_weekly

if __name__ == "__main__":
    LLS = LactationsLogStandard()
