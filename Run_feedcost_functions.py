'''Run_feedcost_functions.py'''


from feed_functions.feedcost_basics import Feedcost_basics
from feed_functions.feedcost_beans import Feedcost_beans
from feed_functions.feedcost_cassava import Feedcost_cassava
from feed_functions.feedcost_corn import Feedcost_corn
from feed_functions.feedcost_CP_005_21P import Feedcost_CP_005_21P
from feed_functions.feedcost_CP_milk2 import Feedcost_CP_milk2
from feed_functions.feedcost_total import Feedcost_total
# from feed_functions.heifer_cost_model import HeiferCostModel



class RunFeedcostFunctions:
    def __init__(self):

        self.feedcost_basics = Feedcost_basics()
        self.feedcost_beans = Feedcost_beans()
        self.feedcost_cassava = Feedcost_cassava()
        self.feedcost_corn = Feedcost_corn()
        self.feedcost_CP_005_21P = Feedcost_CP_005_21P()
        self.feedcost_CP_milk2 = Feedcost_CP_milk2()
        self.feedcost_total = Feedcost_total()
        # self.heifer_cost_model = HeiferCostModel()
            
if __name__ == "__main__":
    RunFeedcostFunctions()