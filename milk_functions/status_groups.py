'''milk_functions\\status_groups.py'''

import pandas as pd
import numpy as np
# from datetime import datetime, timedelta 

from CreateStartDate import DateRange
from milk_functions.statusData import StatusData
from MilkBasics import MilkBasics
from insem_functions.insem_ultra_basics import InsemUltraBasics
from milk_functions.WetDry import WetDry


class statusGroups:
    def __init__ (self):
        
        self.SD     = StatusData()
        self.CSD    = DateRange()
        self.MB     = MilkBasics()
        self.IUB    = InsemUltraBasics()
        self.WD     = WetDry()
        
        self.DRM    = self.CSD.date_range_monthly_data
        self.startdate = self.CSD.startdate
        
        [self.fresh, self.group_A, 
         self.group_B, self.fresh_ids, 
         self.group_A_ids, self.group_B_ids] = self.create_groups()
        
        [self.fresh_monthly, self.group_A_monthly, 
        self.group_B_monthly] = self.create_monthly()
        
        self.write_to_csv()
        
        
        
    def create_groups (self):
        
        # get list of milkers - this code divides that into groups A and B

        # last_calf_age = self.IUB.lastcalf_age
        # milk1 = self.MB.milk
        wet1 = self.WD.wdd.loc[self.startdate:,:]
        
    
        fresh,      groupA,         groupB          = [],[],[]
        fresh_ids1, groupA_ids1,    groupB_ids1,    = [],[],[]
        fresh_ids,  groupA_ids,     groupB_ids,     = [],[],[]
        F_ids,      A_ids,          B_ids           = [],[],[]
        
        freshx,     groupAx,        groupBx = 0,0,0


                
        for date in wet1.index:
            date1   = pd.Timestamp(date)
            lastday = self.MB.lastday
            # gap     = (lastday - date).days
            # milk    = milk1.loc[date1]
            wet     = wet1
            j1 = 0
            
            for i in wet1.columns:
                j1 = wet.loc[date1, i]
                
                
                if not pd.isna(j1) :
                    days1 = j1
                    j = int(j1) - 1

                    if days1 < 10 :
                        freshx += 1
                        F_ids = i

                    elif days1 >=10  and j1 > 14:   #sets min milk prod for groups a/b 
                        groupAx += 1
                        A_ids = i

                    elif days1 >= 70 and j1 <= 14:
                        groupBx += 1
                        B_ids = i
                    
                    i += 1
                
                
                if F_ids:    
                    fresh_ids1.append( F_ids )               
                
                if A_ids:    
                    groupA_ids1.append( A_ids )
                
                if B_ids:                    
                    groupB_ids1.append( B_ids )
                    
                F_ids, A_ids, B_ids = [],[],[]
                
            fresh.  append( [date1, freshx] )
            groupA. append( [date1, groupAx] )
            groupB. append( [date1, groupBx] )
            
            fresh_ids   .append([ date1] + fresh_ids1)
            groupA_ids  .append([ date1] + groupA_ids1)
            groupB_ids  .append([ date1] + groupB_ids1)
            
            freshx  = 0            
            groupAx = 0
            groupBx = 0
            fresh_ids1, groupA_ids1, groupB_ids1 = [],[],[]
            

        self.fresh   = pd.DataFrame(fresh, columns=['date', 'fresh'])            
        self.group_A = pd.DataFrame(groupA, columns=['date', 'groupA'])
        self.group_B = pd.DataFrame(groupB, columns=['date', 'groupB'])
        
        self.fresh = self.fresh.set_index('date', drop=True)
        self.group_A = self.group_A.set_index('date', drop=True)
        self.group_B = self.group_B.set_index('date', drop=True)                      
        
        
        self.fresh_ids   = pd.DataFrame(fresh_ids)
        self.group_A_ids = pd.DataFrame(groupA_ids)
        self.group_B_ids = pd.DataFrame(groupB_ids)
        
        return self.fresh, self.group_A, self.group_B, self.fresh_ids, self.group_A_ids, self.group_B_ids
        
    
    def create_monthly (self):
        
        self.fresh_monthly = self.fresh.copy()
        self.group_A_monthly = self.group_A.copy()
        self.group_B_monthly = self.group_B.copy()   
        
        self.fresh_monthly['year']    = self.fresh_monthly.index.year
        self.fresh_monthly['month']   = self.fresh_monthly.index.month
        self.fresh_monthly['days']    = self.fresh_monthly.index.days_in_month
    
        
        
        self.group_A_monthly['year']    = self.group_A_monthly.index.year
        self.group_A_monthly['month']   = self.group_A_monthly.index.month
        self.group_A_monthly['days']    = self.group_A_monthly.index.days_in_month

        
        self.group_B_monthly['year']    = self.group_B_monthly.index.year
        self.group_B_monthly['month']   = self.group_B_monthly.index.month
        self.group_B_monthly['days']    = self.group_B_monthly.index.days_in_month
              
        
        self.fresh_monthly   = self.fresh_monthly   .groupby(['year', 'month', 'days']).agg({'fresh': 'mean'}).reset_index()
        self.group_A_monthly = self.group_A_monthly .groupby(['year', 'month', 'days']).agg({'groupA': 'mean'}).reset_index()
        self.group_B_monthly = self.group_B_monthly .groupby(['year', 'month', 'days']).agg({'groupB': 'mean'}).reset_index()
        
        
        return self.fresh_monthly, self.group_A_monthly, self.group_B_monthly
    
    def write_to_csv (self):
        

        self.fresh  .to_csv('F:\\COWS\\data\\status\\fresh.csv', index=False)        
        self.group_A.to_csv('F:\\COWS\\data\\status\\group_A.csv', index=False)
        self.group_B.to_csv('F:\\COWS\\data\\status\\group_B.csv', index=False)
        
        self.fresh_ids  .to_csv('F:\\COWS\\data\\status\\fresh_ids.csv', index=False)
        self.group_A_ids.to_csv('F:\\COWS\\data\\status\\group_A_ids.csv', index=False)
        self.group_B_ids.to_csv('F:\\COWS\\data\\status\\group_B_ids.csv', index=False)

        self.fresh_monthly  .to_csv('F:\\COWS\\data\\status\\fresh_monthly.csv', index=False)        
        self.group_A_monthly.to_csv('F:\\COWS\\data\\status\\group_A_monthly.csv', index=False)
        self.group_B_monthly.to_csv('F:\\COWS\\data\\status\\group_B_monthly.csv', index=False)
    
if __name__ == "__main__":
    sg = statusGroups()