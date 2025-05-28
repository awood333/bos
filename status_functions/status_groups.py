'''milk_functions\\status_groups.py'''

import pandas as pd

from CreateStartDate import DateRange
from status_functions.statusData import StatusData
from status_functions.WetDry import WetDry

from MilkBasics import MilkBasics
from insem_functions.insem_ultra_basics import InsemUltraBasics


class statusGroups:
    def __init__ (self):
        
        SD     = StatusData()
        self.CSD    = DateRange()
        self.MB     = MilkBasics()
        self.IUB    = InsemUltraBasics()
        self.WD     = WetDry()
        
        self.DRM    = self.CSD.date_range_monthly_data
        self.startdate = self.CSD.startdate
        
        self.herd_daily = SD.herd_daily
        
        [self.fresh_ids, self.group_A_ids, self.group_B_ids,
        self.all_groups_count]   = self.create_groups()
        
        self.all_groups_count_monthly = self.create_monthly()
        
        self.write_to_csv()
        
        
        
    def create_groups (self):

        wet1 = self.WD.wdd.loc[self.startdate:, :]
        # wet1 = self.WD.wdd.iloc[-1:, :]
        milk1 = self.MB.data['milk'].loc[self.startdate:, :]
        
    
        fresh,      groupA,         groupB          = [],[],[]
        fresh_ids1, groupA_ids1,    groupB_ids1,    = [],[],[]
        fresh_ids,  groupA_ids,     groupB_ids,     = [],[],[]
        F_ids,      A_ids,          B_ids           = [],[],[]
        
        freshx,     groupAx,        groupBx = 0,0,0


                
        for date in wet1.index:
            date1   = pd.Timestamp(date)
            wet     = wet1
            j1 = 0
            
            for i in wet1.columns:
                j1 = wet.loc[date1, i]
                m1 = milk1.loc[date1, str(i)]
                
                if not pd.isna(m1):
                    m1 = milk1[str(i)].rolling(window=10).mean().loc[date1]
                    days1 = j1
                    # j = int(j1) - 1

                    if 0 < days1<= 10 :
                        freshx += 1
                        F_ids = i

                    elif days1 >10  and m1 >= 15:   #sets min milk prod for groups a/b 
                        groupAx += 1
                        A_ids = i

                    elif days1 >= 10 and m1 < 15:
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
            

        fresh_count   = pd.DataFrame(fresh, columns=['date', 'fresh'])            
        group_A_count = pd.DataFrame(groupA, columns=['date', 'groupA'])
        group_B_count = pd.DataFrame(groupB, columns=['date', 'groupB'])
        
        fresh_count   = fresh_count.set_index('date', drop=True)
        group_A_count = group_A_count.set_index('date', drop=True)
        group_B_count = group_B_count.set_index('date', drop=True)
                              
        self.all_groups_count = pd.concat((fresh_count, group_A_count, group_B_count), axis=1)
        
        self.fresh_ids   = pd.DataFrame(fresh_ids).set_index(0)
        self.group_A_ids = pd.DataFrame(groupA_ids).set_index(0)
        self.group_B_ids = pd.DataFrame(groupB_ids).set_index(0)
        
        return [self.fresh_ids, self.group_A_ids, self.group_B_ids,
                self.all_groups_count]
        
        
        
        
    
    def create_monthly (self):
        
        self.all_groups_count_monthly1 = self.all_groups_count.copy() 
        
        self.all_groups_count_monthly1['year']    = self.all_groups_count_monthly1.index.year
        self.all_groups_count_monthly1['month']   = self.all_groups_count_monthly1.index.month
        self.all_groups_count_monthly1['days']    = self.all_groups_count_monthly1.index.days_in_month
    
        
        self.all_groups_count_monthly   = self.all_groups_count_monthly1.groupby(['year', 'month', 'days']).agg(
            {'fresh': 'mean',
             'groupA': 'mean',
             'groupB': 'mean'
             }
            ).reset_index()
        
        
        return self.all_groups_count_monthly
    
    def write_to_csv (self):
        
        self.reconcile_counts = pd.concat((self.all_groups_count, self.herd_daily), axis=1)
        

        self.reconcile_counts  .to_csv('F:\\COWS\\data\\status\\reconcile_counts.csv')        
        
        self.fresh_ids  .to_csv('F:\\COWS\\data\\status\\fresh_ids.csv')
        self.group_A_ids.to_csv('F:\\COWS\\data\\status\\group_A_ids.csv')
        self.group_B_ids.to_csv('F:\\COWS\\data\\status\\group_B_ids.csv')

        self.all_groups_count_monthly  .to_csv('F:\\COWS\\data\\status\\all_groups_count_monthly.csv')        
    
if __name__ == "__main__":
    sg = statusGroups()