'''
feed_inventory.py
'''
import pandas as pd
import os
import datetime
from CreateStartDate import DateRange
# from feed_cost  import FeedCost
# from insem_ultra import InsemUltraData
# from status import StatusData


class FeedInventory():
    def __init__(self):
        
        DR    = DateRange()   

        self.maxdate     = datetime.datetime.today()
        self.stopdate    = self.maxdate
        self.startdate   = DR.startdate
        self.date_range  = pd.date_range(self.startdate, self.stopdate, freq='D')
        self.elapsed_days = len(self.date_range)
        
        # functions
        self.corn_inventory     = self.create_corn_inventory()
        self.cassava_inventory  = self.create_cassava_inventory()      
        self.beans_inventory    = self.create_beans_inventory()
        self.straw_inventory    = self.create_straw_inventory()
        self.agg_inventory      = self.create_agg_inventory()


    def create_corn_inventory(self):
        return create_inventory(self, 'corn')
    
    def create_cassava_inventory(self):
        return create_inventory(self, 'cassava')

    def create_beans_inventory(self):
        return create_inventory(self, 'beans')
    
    def create_straw_inventory(self):
        return create_inventory(self, 'straw')
    
    def create_agg_inventory(self):
        beans   = self.beans_inventory
        corn    = self.corn_inventory
        cassava = self.cassava_inventory
        straw   = self.straw_inventory
        agg_inventory1 = [(beans, corn, cassava, straw)]
        agg_inventory  = pd.DataFrame(agg_inventory1, columns=['beans', 'corn', 'cassava', 'straw'])
        agg_inventory.to_csv('F:\\COWS\\data\\feed_data\\inventory\\agg_inventory.csv')
        x=1
        return agg_inventory
    


def create_inventory(self, feed_type):
        base_path1      = 'F:\\COWS\\data\\feed_data\\inventory'
        inventory_path  = os.path.join  (base_path1, f'{feed_type}_inventory.csv')
        inventory       = pd.read_csv   (inventory_path, index_col=0, header=0, 
                                         parse_dates=['date_filled', 'date_opened', 'date_finished'])    
        
        base_path2      = 'F:\\COWS\\data\\feed_data'
        cost_path       = os.path.join  (base_path2, f'{feed_type}_cost.csv')
        cost            = pd.read_csv   (cost_path, index_col=0, header=0, 
                                         parse_dates=['datex'])

        idxval          = inventory['date_opened'].idxmax()      #idxmax finds the index of the max value in that field
        last_open_date  = inventory.loc[idxval, 'date_opened']
        this_bunker     = inventory.loc[idxval,'bunker#']
        this_amount     = inventory.loc[idxval,'kg']
        elapsed_days   = (self.maxdate - last_open_date).days
        # elapsed_days    = int(elapsed_days1)

        consumption1    = cost['total kgperday']
        consumption2    = consumption1 * elapsed_days
        current_inventory       = this_amount - consumption2
        
        varname         = f"{feed_type}_inventory"
        varname         = current_inventory.iloc[-1]
        
        return varname

if __name__ == "__main__":
    FI = FeedInventory()
