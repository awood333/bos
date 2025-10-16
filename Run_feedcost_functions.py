'''Run_feedcost_functions.py'''

class RunFeedcostFunctions:
    def __init__(self):
        from milk_basics import MilkBasics
        from feed_functions.feedcost_basics import Feedcost_basics
        from feed_functions.feedcost_beans import Feedcost_beans
        from feed_functions.feedcost_cassava import Feedcost_cassava
        from feed_functions.feedcost_corn import Feedcost_corn
        from feed_functions.feedcost_total import Feedcost_total
        # from feed_functions.heifer_cost_model import HeiferCostModel
            
    def run_feedcost_functions():
        feedcost_basics = Feedcost_basics()
        feedcost_beans = Feedcost_beans()
        feedcost_cassava = Feedcost_cassava()
        feedcost_corn = Feedcost_corn()
        feedcost_total = Feedcost_total()
        # heifer_cost_model = HeiferCostModel()
            
if __name__ == "__main__":
    RunFeedcostFunctions()