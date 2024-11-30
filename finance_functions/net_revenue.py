'''finance_functions\\net_revenue.py'''

import pandas as pd
import numpy as np
from finance_functions.milk_income import MilkIncome
from feed_functions.feed_cost_basics import FeedCostBasics
from milk_functions.WetDry          import WetDry
from milk_functions.statusData     import StatusData
from milk_functions.status_groups   import statusGroups

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
        a = self.SG.group_A_ids.iloc[-1:,1:].values
        b = self.SG.group_B_ids.iloc[-1:,1:].values
        f = self.SG.fresh_ids.iloc[-1:,1:].values
        status_groups_list1 = np.concatenate((a, b, f), axis=1)
        status_groups_list2 = pd.DataFrame(status_groups_list1).dropna(axis=1, how='all')
        status_groups_list3 = status_groups_list2.T
        status_groups_list = status_groups_list3.sort_values(by=status_groups_list3.columns[0]).reset_index(drop=True)
        
        m = pd.DataFrame(self.SD.milker_ids[-1])
        
        self.milker_lists_comp = pd.merge(status_groups_list, m, left_index=True, right_index=True)

        return self.milker_lists_comp
        

    def create_net_revenue(self):

        nr = pd.concat((self.SG.group_A, self.SG.group_B, self.SD.herd_daily, self.FCB.feedcost_daily ), axis=1)
        nr = pd.concat((nr, self.MI.income_daily['daily_avg_net']), axis=1)
        
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        print('nr ', nr.iloc[:5,:])
        
        nr['A_agg_cost'] = nr['groupA_count'] * nr['cost A'] * nr.index.get_level_values('days')
        nr['B_agg_cost'] = nr['groupB_count'] * nr['cost B'] * nr.index.get_level_values('days')
        nr['total cost'] = nr['A_agg_cost'] + nr['B_agg_cost'] + nr['dry 15pct agg cost']
        nr['net revenue'] = nr['gross_baht'] - nr['total cost']
        
        nr = nr.drop(columns=(['milkers', 'milkers agg cost']))

        self.net_revenue = nr.loc[:,[
                             
                            'gross_baht',
                            'cost A',
                            'cost B',
                            'dry cost',
                            'groupA_count',
                            'groupB_count',
                            'dry_15pct',
                            'A_agg_cost',
                            'B_agg_cost',
                            'dry 15pct agg cost',
                            'total cost',
                            'net revenue'
            ]
        ]
        
        return self.net_revenue
    
    def write_to_csv(self):
        self.net_revenue.to_csv('F:\\COWS\\data\\feed_data\\feedcost_by_group\\net.csv')
        self.milker_lists_comp.to_csv('F:\\COWS\\data\\status\\milker_lists_comp.csv')
if __name__ == "__main__":
    NetRevenue()            