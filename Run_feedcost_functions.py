'''Run_feedcost_functions.py'''


from feed_functions.feedcost_basics import Feedcost_basics
from feed_functions.feedcost_data   import FeedcostData
from feed_functions.feedcost_total  import Feedcost_total
# from feed_functions.heifer_cost_model import HeiferCostModel



class RunFeedcostFunctions:
    def __init__(self):

        self.feedcost_basics    = Feedcost_basics()
        self.feedcost_data      = FeedcostData()
        self.feedcost_total     = Feedcost_total()
        # self.heifer_cost_model = HeiferCostModel()

        # Call processing methods to update CSVs
        self.feedcost_basics.load_and_process()
        self.feedcost_data.load_and_process()
        self.feedcost_total.load_and_process()        
            
if __name__ == "__main__":
    RunFeedcostFunctions()