'''Lactations'''
import inspect
import pandas as pd
import numpy as np
from container import get_dependency
from persistent_container_service import ContainerClient



class LactationsLogStandard:
    def __init__(self):
        print(f"LactationsLogStandard instantiated by: {inspect.stack()[1].filename}")
        self.MB = None
        self.LB = None
        self.L = None
        self.LTL = None
        self.LW = None
        self.m1_log_daily = None
        self.m1_log_weekly = None

    def load_and_process(self):
        client = ContainerClient()
        self.MB = client.get_dependency('milkbasics')
        self.LB = client.get_dependency('lactationbasics')
        self.L  = client.get_dependency('lactations')
        self.LTL= client.get_dependency('thislactation')
        self.LW = client.get_dependency('weeklylactations')
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
