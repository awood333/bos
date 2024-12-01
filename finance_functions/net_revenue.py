'''finance_functions\\net_revenue.py'''

import pandas as pd
import numpy as np
import warnings

from finance_functions.milk_income import MilkIncome
from feed_functions.feed_cost_basics import FeedCostBasics
from milk_functions.WetDry          import WetDry
from milk_functions.statusData     import StatusData
from milk_functions.status_groups   import statusGroups

warnings.simplefilter('always', FutureWarning)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

class NetRevenue:
    def __init__ (self):
        
        self.MI = MilkIncome()
        self.FCB = FeedCostBasics()
        self.WD     = WetDry()
        self.SD     = StatusData()
        self.SG     = statusGroups()

        self.milker_lists_comp = self.compare_lists()
        self.net_revenue = self.create_net_revenue()
        self.write_to_csv()
        
    def compare_lists(self):
        
        # from status_groups
        a = self.SG.group_A_ids.iloc[-1:,1:].values
        b = self.SG.group_B_ids.iloc[-1:,1:].values
        f = self.SG.fresh_ids.iloc[-1:,1:].values
        status_groups_list1 = np.concatenate((a, b, f), axis=1)
        status_groups_list2 = pd.DataFrame(status_groups_list1).dropna(axis=1, how='all')
        status_groups_list3 = status_groups_list2.T
        status_groups_list = status_groups_list3.sort_values(by=status_groups_list3.columns[0]).reset_index(drop=True)
        
        
        # from status_data
        m = pd.DataFrame(np.array(self.SD.milker_ids[-1]))
        
        
        # merge 
        self.milker_lists_comp = pd.merge(status_groups_list, m, left_index=True, right_index=True)
        if len(m) != len(status_groups_list):
            print("look at F:\\COWS\\data\\status\\milker_lists_comp.csv")

        return self.milker_lists_comp
        

    def create_net_revenue(self):
        

        nr1 = pd.concat((self.SG.group_A, self.SG.group_B, self.SG.fresh, self.SD.herd_daily, self.FCB.feedcost_daily ), axis=1)
        nr2 = pd.concat((nr1, self.MI.income_daily['daily_avg_net']), axis=1)
        nr2['days'] = nr2.index.days_in_month
        

        # print('nr2 ', nr2.iloc[:5,:])

        nr2['fresh_agg_cost']   = nr2['fresh']      * nr2['totalcostA'] 
        nr2['A_agg_cost']       = nr2['groupA']     * nr2['totalcostA'] 
        nr2['B_agg_cost']       = nr2['groupB']     * nr2['totalcostB'] 
        nr2['D15_agg_cost']     = nr2['dry_15pct']  * nr2['totalcostD'] 
        
        nr2['total cost'] = nr2['fresh_agg_cost'] + nr2['A_agg_cost'] + nr2['B_agg_cost'] + nr2['D15_agg_cost']
        nr2['net revenue'] = nr2['daily_avg_net'] - nr2['total cost']
        
        # nr2 = nr2.drop(columns=(['milkers', 'milkers agg cost']))
        nr3 = nr2.rename(columns={
            'groupA' : "A count",
            'groupB' : 'B count',
            'fresh'  : 'fresh count',
            'dry'     : 'dry_all count',
            'dry_15pct' : 'D15 count',
            'totalcostA' : "cost A",
            'totalcostB' : "cost B",
            'totalcostD' : "cost D",
            'daily_avg_net' : 'baht'
            
            
        }) 

        self.net_revenue = nr3.loc[:,[
                            'milkers',
                            'fresh count',
                            'A count',
                            'B count',
                            'dry_all count',
                            'D15 count',
                            'alive',
                            'cost A',
                            'cost B',
                            'cost D',
                            'fresh_agg_cost',
                            'A_agg_cost',
                            'B_agg_cost',
                            'D15_agg_cost',
                            'total cost',
                            'baht',                            
                            'net revenue'
            ]
        ]
        
        return self.net_revenue
    
    def write_to_csv(self):
        self.net_revenue.to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\net.csv')
        self.milker_lists_comp.to_csv('F:\\COWS\\data\\status\\milker_lists_comp.csv')
        
        
if __name__ == "__main__":
    NetRevenue()            