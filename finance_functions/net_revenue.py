'''finance_functions\\net_revenue.py'''

import pandas as pd
import numpy as np
import datetime
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


        self.milker_lists_comp      = self.compare_lists()
        self.net_revenue            = self.create_net_revenue()
        self.net_revenue_model,self.net_revenue_model2      = self.create_net_revenue_model()        
        self.net_revenue_monthly    = self.create_monthly_net()
        self.write_to_csv()
        
    def compare_lists(self):
        
        #compare the last rows
        
        # from status_groups (via wet_dry)
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
        self.milker_lists_comp.columns = ['SGlist', 'milkers']
        
        if len(m) != len(status_groups_list):
            print("look at F:\\COWS\\data\\status\\milker_lists_comp.csv")
        if len(m) == len(status_groups_list):
            print("lists agree")

        return self.milker_lists_comp
    
    
   

    def create_net_revenue(self):
        

        nr1 = pd.concat((self.SG.group_A, self.SG.group_B, self.SG.fresh, self.SD.herd_daily, self.FCB.feedcost_daily ), axis=1)
        nr1 = pd.concat((nr1, self.MI.income_daily['daily_avg_net']), axis=1)
        nr1['days'] = nr1.index.days_in_month
        

        # print('nr1 ', nr1.iloc[:5,:])

        nr1['fresh_agg_cost']   = nr1['fresh']      * nr1['totalcostA'] 
        nr1['A_agg_cost']       = nr1['groupA']     * nr1['totalcostA'] 
        nr1['B_agg_cost']       = nr1['groupB']     * nr1['totalcostB'] 
        nr1['D15_agg_cost']     = nr1['dry_15pct']  * nr1['totalcostD'] 
        
        nr1['total feed cost'] = nr1['fresh_agg_cost'] + nr1['A_agg_cost'] + nr1['B_agg_cost'] + nr1['D15_agg_cost']
        nr1['net revenue'] = nr1['daily_avg_net'] - nr1['total feed cost']
        
        # nr1 = nr1.drop(columns=(['milkers', 'milkers agg cost']))
        nr3 = nr1.rename(columns={
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
                            'total feed cost',
                            'baht',                            
                            'net revenue'
            ]
        ]
        
        
        return self.net_revenue
    
    
    def create_net_revenue_model(self):


        nr1 = self.FCB.feedcost_daily.iloc[-1:,:].copy()
        
        
        nr1['D15_cost']     = .15 * nr1['totalcostD'] 
        
        nr1['total feed cost'] = (nr1['totalcostA'] * 30 *7) + ( nr1['totalcostB'] * 14 * 7)   +   (nr1['D15_cost'] * 8 * 7)
        nr1['avg feedcost day'] = nr1['total feed cost'] / 365
        nr1['avg feedcost month'] = nr1['total feed cost'] / 12
        nr1['avg feedcost annual'] = nr1['total feed cost']
        nr1['liters_lact'] = 4500
        nr1['baht_liter'] = 22
        nr1['gross_revenue'] = nr1['liters_lact'] * nr1['baht_liter']
        nr1['lactations'] = 3
        nr1['cow cost'] = 65000
        nr1['cow depreciation'] = nr1['cow cost'] / nr1['lactations']       
        nr1['net revenue'] = nr1['gross_revenue'] - nr1['total feed cost']
        nr1['op profit'] = nr1['net revenue'] -  nr1['cow depreciation']
        nr1['op costs'] = 1500000
        nr1['min herd size'] = nr1['op costs']/nr1['op profit']



        nr2 = nr1.rename(columns={   
            'totalcostA' : "cost A",
            'totalcostB' : "cost B"
        }) 

        nr3 = nr2.loc[:,[
                            'cost A',
                            'cost B',
                            'D15_cost',
                            'avg feedcost day',
                            'avg feedcost month',
                            'avg feedcost annual',
                            'liters_lact',
                            'baht_liter',
                            'gross_revenue',
                            'total feed cost',
                            'net revenue'                  
            ]
        ]
         
        nrx1 = nr2.loc[:,[
                      'net revenue',
                        'lactations',
                        'cow cost',
                        'cow depreciation',  
                        'op profit',
                        'op costs',
                        'min herd size'     ]
        ]
         
        nr4 = nr3.T
        nrx2 = nrx1.T
        
        self.net_revenue_model =  pd.DataFrame(nr4)
        self.net_revenue_model2 =  pd.DataFrame(nrx2)
        
        return  self.net_revenue_model, self.net_revenue_model2        
    
    
    def create_monthly_net(self):
    
        nrm1 = self.net_revenue
        nrm1['year'] = nrm1.index.year
        nrm1['month'] = nrm1.index.month
        nrm1['days'] = nrm1.index.daysinmonth
        
        self.net_revenue_monthly = nrm1.groupby(['year','month','days']).sum()

        return self.net_revenue_monthly
    
    def write_to_csv(self):
        self.net_revenue            .to_csv('F:\\COWS\\data\\PL_data\\net.csv')
        self.milker_lists_comp      .to_csv('F:\\COWS\\data\\status\\milker_lists_comp.csv')
        self.net_revenue_monthly    .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_monthly.csv')
        self.net_revenue_model      .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_model.csv')
        self.net_revenue_model2     .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_model2.csv')
        
        
        
        
if __name__ == "__main__":
    NetRevenue()            