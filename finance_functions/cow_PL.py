'''
cow_PL.py
'''
import pandas as pd

from CreateStartDate import DateRange
from feed_functions.feed_cost_basics import FeedCostBasics
from finance_functions.create_milk_revenue import Revenue

class CowPL:
    def __init__(self):
        
        DR = DateRange()
        FCB = FeedCostBasics()
        R   = Revenue()
        self.feed_cost = FCB.gtc
        self.milk_gross_inc = R.income
        
    def createGrossRevenue(self):
        fc = self.feed_cost
        rev = self.milk_gross_inc.loc[(2024, 1):,:]


if __name__ == "__main__":
    cow_pl = CowPL()
    cow_pl.createGrossRevenue()