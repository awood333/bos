'''run_cost_PL.py'''
from feed_cost          import FeedCost 
from financial_stuff    import FinancialStuff
from cow_PL             import Cow_PL
from feed_inventory     import FeedInventory


fc      = FeedCost() 
fc.create_write_to_csv
 
cpl     = Cow_PL()
cpl.create_write_to_csv()

fi      = FeedInventory()
fi.create_agg_inventory() 

fs      = FinancialStuff()
fs.create_write_to_csv()