'''finance_functions.CapexProjects.py'''

import pandas as pd
from datetime import datetime as dt
from finance_functions.capex.CapexBasics import CapexBasics

class CapexProjects:
    def __init__(self):
        CB = CapexBasics()
        self.capex = CB.capex_details
        self.old_capex                  = pd.read_csv("F:\\COWS\\data\\finance\\capex\\old capex\\old_capex_list.csv", index_col='datex')
        self.old_capex.index = pd.to_datetime(self.old_capex.index).date
        
        self.project_completion_dates   = pd.read_csv("F:\\COWS\\data\\finance\\capex\\projects\\project_completion_dates\\project_completion_dates.csv")
        self.project_completion_dates['completion_date'] = pd.to_datetime(self.project_completion_dates['completion_date']).dt.date
        
        self.old_capex_project_names = [
            'gatoum',
            'cowbarn_1', 
            'cowbarn_2', 
            'cowbarn_3',
            'silage',
            'house',              
            'heifer_barn',
            'storage', 
            'misc'
        ]
        
        self.capex_project_names    = [
            'cowboy_fence',
            'cowboy_fence_brahman',
            'delaval',
            'milking_parlor',
            'new_barn',
            'new_barn_equipment',
            'solar_1',
            'solar_2',
            'solar_3'
            ]
        
        self.large_equipment_names = [
            'tractors',
            'TMR',
            'allflex',
            'misc_equipment'
        ]
        
        self.template_cols = [
            'project name','completion date','equipment','materials','labor'
        ]
        
        
        self.ptoc = self.create_project_details_value()
        
      
        
    def create_project_details_value(self):
        
        project_totals_old_capex = pd.DataFrame()
        
        template_df = pd.DataFrame(columns=self.template_cols)
  
        for name in self.old_capex_project_names:
                
            
            project_name = name
            
            pcd = self.project_completion_dates
            completion_date = pcd.loc[pcd['project_name'] ==project_name,
                    'completion_date'].values[0]
            
            
            capex1 = self.old_capex
            capex2 = capex1.loc[(capex1['descr 3'] == project_name), :]
            
            # slice data to allow only the original cost data
            
            mp_pivot = pd.pivot_table(capex2,
                    index = ['year','month'],
                    columns= 'descr 1',
                    values= 'amount',
                    aggfunc= 'sum'
                        )

            # row sum column
            mp_pivot['sum'] = mp_pivot.sum(axis=1)
            mp_pivot.reset_index(inplace=True)

            # slice off the year/month cols for the column sum row 
            colsum1 = mp_pivot.iloc[:,2:]
            # create the 'details' df
            colsum_row = pd.Series(colsum1.sum(axis=0))
            colsum_row2 = pd.DataFrame(colsum_row)
            initial_project_details = pd.concat([mp_pivot, colsum_row2], axis=0)            
            
            # copy to avoid overwriting colsum_row
            project_totals_row = colsum_row.copy()
            # create 'value' df
            project_totals_row['completion date'] = completion_date
            project_totals_row['project name'] = name
            project_totals_row2 = pd.DataFrame(project_totals_row)
            
            # ensure df have the same cols
            initial_project_value           = pd.concat([template_df, project_totals_row2.T], axis=0)
            # create 'bldg' for depreciation calc
            initial_project_value['bldg']   = initial_project_value['materials'] + initial_project_value['labor']


            setattr(self, f"{project_name}_initial_project_value", colsum_row2)
            project_totals_old_capex = pd.concat([project_totals_old_capex, initial_project_value], ignore_index=True)

            setattr(self, f"{project_name}_initial_project_details", initial_project_details)

            getattr(self, f"{project_name}_initial_project_details")    .to_csv(f"F:\\COWS\\data\\finance\\capex\\projects\\project_details\\{project_name}_initial_project_details.csv")
 
        self.ptoc = project_totals_old_capex
        project_totals_old_capex.to_csv("F:\\COWS\\data\\finance\\capex\\projects\\project_details\\project_totals_old_capex.csv", index=False)
        
        return self.ptoc
       
       
if __name__ == "__main__":
    CapexProjects()
        