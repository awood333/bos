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
        self.net_revenue_monthly    = self.create_monthly_net()
      
        self.scenario_report      = self.create_net_revenue_scenarios()        
        self.write_to_csv()
        
    def compare_lists(self):
        
        #compare the last rows
        
        filepath    = "F:\\COWS\\data\\daily_milk.ods"
        sheetname   = 'AM_wy'
        data        = pd.read_excel(filepath,sheet_name=sheetname, skiprows=3, engine='odf')
        dailymilk_lastcol = data.iloc[:,-1].copy().sort_values().reset_index(drop=True)
        
        
        # from status_groups (via status_grous)
        a = self.SG.group_A_ids.iloc[-1:,1:].values
        b = self.SG.group_B_ids.iloc[-1:,1:].values
        f = self.SG.fresh_ids.iloc[-1:,1:].values

        # Convert to DataFrames and pad with NaN to match lengths
        a_df = pd.DataFrame(a).T
        b_df = pd.DataFrame(b).T
        f_df = pd.DataFrame(f).T

        # Concatenate along columns, padding with NaN where necessary
        status_groups = pd.concat([a_df, b_df, f_df], axis=1, ignore_index=True)        
        status_groups.columns=['a','b','f']

        status_groups_list1 = np.concatenate((a, b, f), axis=1)
        status_groups_list2 = pd.DataFrame(status_groups_list1).dropna(axis=1, how='all')
        status_groups_list3 = status_groups_list2.T
        status_groups_list = status_groups_list3.sort_values(by=status_groups_list3.columns[0]).reset_index(drop=True)
        status_groups_list.columns = ['sg_list']
        
        # from status_data
        m = pd.DataFrame(np.array(self.SD.milker_ids[-1]),  columns=['sd_milkers_id'])
        
        # concat
        self.milker_lists_comp  = pd.concat( [a_df, b_df, f_df, status_groups_list, dailymilk_lastcol,  m], axis=1)
        
        
        
        # if len(m) != len(status_groups_list):
        #     print("look at F:\\COWS\\data\\status\\milker_lists_comp.csv")
        # if len(m) == len(status_groups_list):
        #     print("lists agree")

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
        nr1['total feed cost']  = nr1['fresh_agg_cost'] + nr1['A_agg_cost'] + nr1['B_agg_cost'] + nr1['D15_agg_cost']
        nr1['net rev_yr']       = nr1['daily_avg_net'] - nr1['total feed cost']
        
        # nr1 = nr1.drop(columns=(['milkers', 'milkers agg cost']))
        nr2 = nr1.rename(columns={
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

        self.net_revenue = nr2.loc[:,[
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
                            'net rev_yr'
            ]
        ]
        
        
        return self.net_revenue
    
    
    def create_monthly_net(self):
    
        nrm1 = self.net_revenue
        nrm1['year'] = nrm1.index.year
        nrm1['month'] = nrm1.index.month
        nrm1['days'] = nrm1.index.daysinmonth
        
        self.net_revenue_monthly = nrm1.groupby(['year','month','days']).sum()

        return self.net_revenue_monthly
    
    def create_net_revenue_scenarios(self):

        nrm = self.FCB.feedcost_daily.iloc[-1:,:].copy()
        nrm = nrm.rename(columns={   
            'totalcostA' : "cost A",
            'totalcostB' : "cost B",
            'totalcostD' : 'cost D'
        }) 
        
        scenarios = {
            'lactations_num'          : [3,4,5]  ,
            'lact dur_wks'            : [52,56,60],
            'A_dur'                   : [30,30,30],
            'B_dur'                   : [14,18,22],
            'D_dur'                   : [8,8,8],
            'milk_price'              : [21.75, 21.75, 21.75],
            'liters_lact'             : [4000,4250,4500],
            'cow_cost'                : [65000,65000,65000],
            'op_costs'                : [1500000,150000,150000]
        }
            
        scenario_names = ['1','2','3']
        reports = []
        
        for i, scenario in enumerate(scenario_names):
            nrm_scenario = nrm.copy()          


            nrm_scenario['D15 cost'] = .15 * nrm_scenario['cost D']
            nrm_scenario['scenario'] = scenario
            nrm_scenario['lact dur_wks'] = scenarios['lact dur_wks'][i]
            # nrm_scenario['lactations'] = scenarios['lactations_num'][i]
            nrm_scenario['A_dur'] = scenarios['A_dur'][i]
            nrm_scenario['B_dur'] = scenarios['B_dur'][i]
            nrm_scenario['D_dur'] = scenarios['D_dur'][i]       
                 
            nrm_scenario['baht_liter']  = scenarios['milk_price'][i]
            nrm_scenario['liters_lact'] = scenarios['liters_lact'][i]
            # nrm_scenario['cow cost'] = scenarios['cow_cost'][i]
            # nrm_scenario['op_costs'] = scenarios['op_costs'][i]

            nrm_scenario['total feed cost'] = (nrm_scenario['cost A'] * scenarios['A_dur'][i] * 7) + (nrm_scenario['cost B'] * scenarios['B_dur'][i] * 7) + (nrm_scenario['D15 cost'] * scenarios['D_dur'][i] * 7)
            nrm_scenario['avg feedcost day'] = nrm_scenario['total feed cost'] / (nrm_scenario  ['lact dur_wks'] * 7)
            nrm_scenario['avg feedcost week'] = nrm_scenario['avg feedcost day']  * 7
            nrm_scenario['avg feedcost month'] = nrm_scenario['avg feedcost day'] * 30.25
            nrm_scenario['feedcost per lact'] = nrm_scenario['total feed cost']
            
            nrm_scenario['milk income'] = scenarios['milk_price'][i] * scenarios['liters_lact'][i]
            nrm_scenario['net_revenue'] = nrm_scenario['milk income'] - nrm_scenario['feedcost per lact']

            reports.append(nrm_scenario)
            
        self.scenario_report = pd.concat(reports)
        return  self.scenario_report       
            
    
    
    def write_to_csv(self):
        self.milker_lists_comp      .to_csv('F:\\COWS\\data\\status\\milker_lists_comp.csv')       
        self.net_revenue            .to_csv('F:\\COWS\\data\\PL_data\\net.csv')
        self.net_revenue_monthly    .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_monthly.csv')
        self.scenario_report        .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_scenario_report.csv')
        # self.net_revenue_model      .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_model.csv')
        # self.net_revenue_model2     .to_csv('F:\\COWS\\data\\PL_data\\net_revenue_model2.csv')
        
        
        
        
if __name__ == "__main__":
    NetRevenue()            