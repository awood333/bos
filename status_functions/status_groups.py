'''milk_functions\\status_groups.py'''
import inspect
from datetime import timedelta
import pandas as pd


from date_range import DateRange
from status_functions.statusData import StatusData
from status_functions.wet_dry import WetDry

from milk_basics import MilkBasics
from insem_functions.insem_ultra_basics import InsemUltraBasics



class statusGroups:
    def __init__ (self, date_range=None, status_data=None, wet_dry=None, 
                  milk_basics=None, insem_ultra_basics=None):
        
        print(f"statusGroups instantiated by: {inspect.stack()[1].filename}")
        self.SD     = status_data or StatusData()
        self.CSD    = date_range or DateRange()
        self.MB     = milk_basics or MilkBasics()
        self.IUB    = insem_ultra_basics or InsemUltraBasics()
        self.WD     = wet_dry or WetDry()
        
        self.DRM    = self.CSD.date_range_monthly_data
        self.startdate = self.CSD.startdate
        
        self.herd_daily = self.SD.herd_daily
        
        [self.fresh_ids, self.group_A_ids, self.group_B_ids,
        self.group_C_ids, self.all_groups_count]   = self.create_groups()
        
        self.all_groups_count_monthly = self.create_monthly()
        
        self.write_to_csv()
        
        
        
    def create_groups (self):

        wet1a = self.WD.wdd.loc[self.startdate:, :]
        wetT = wet1a.T
        alive_mask = self.SD.alive_ids.astype(int)
        wet1b = wetT.loc[alive_mask]
        wet = wet1b.T

        milk1 = self.MB.data['milk'].loc[self.startdate:, :]
        
    
        fresh,      groupA,         groupB,         groupC          = [],[],[],[]
        fresh_ids1, groupA_ids1,    groupB_ids1,    groupC_ids1     = [],[],[],[]
        fresh_ids,  groupA_ids,     groupB_ids,     groupC_ids      = [],[],[],[]
        F_ids,      A_ids,          B_ids,          C_ids           = [],[],[],[]
        
        freshx,     groupAx,        groupBx,        groupCx         = 0,0,0,0


                
        for date in wet.index:
            date1   = pd.Timestamp(date)
            date1x = date1 + timedelta(days=10)
            j1 = 0

            
            for i in wet.columns:
                j1 = wet.loc[date1, i]
                m1 = milk1.loc[date1, str(i)]
                
                if not pd.isna(m1):
                    m1 = milk1[str(i)].rolling(window=10).mean().loc[date1]
                    days1 = j1
                    # j = int(j1) - 1

                    if 0 < days1<= 14 :
                        freshx += 1
                        F_ids = i

                    elif days1 >14  and m1 >= 16:   #sets min milk prod for groups a/b 
                        groupAx += 1
                        A_ids = i

                    elif days1 >= 50 and (m1 < 16 and m1 >=11):
                        groupBx += 1
                        B_ids = i

                    elif days1 >= 50 and m1 < 11:
                        groupCx += 1
                        C_ids = i
                    
                    i += 1
                    
                
                if F_ids:    
                    fresh_ids1.append( F_ids )               
                
                if A_ids:    
                    groupA_ids1.append( A_ids )
                
                if B_ids:                    
                    groupB_ids1.append( B_ids )

                if C_ids:                    
                    groupC_ids1.append( C_ids )

                F_ids, A_ids, B_ids, C_ids = [],[],[],[]
                
            fresh.  append( [date1, freshx] )
            groupA. append( [date1, groupAx] )
            groupB. append( [date1, groupBx] )
            groupC. append( [date1, groupCx] )
            
            fresh_ids   .append([ date1] + fresh_ids1)
            groupA_ids  .append([ date1] + groupA_ids1)
            groupB_ids  .append([ date1] + groupB_ids1)
            groupC_ids  .append([ date1] + groupC_ids1)

            freshx  = 0            
            groupAx = 0
            groupBx = 0
            groupCx = 0

            fresh_ids1, groupA_ids1, groupB_ids1, groupC_ids1 = [],[],[],[]
            

        fresh_count   = pd.DataFrame(fresh, columns=['date', 'fresh'])            
        group_A_count = pd.DataFrame(groupA, columns=['date', 'groupA'])
        group_B_count = pd.DataFrame(groupB, columns=['date', 'groupB'])
        group_C_count = pd.DataFrame(groupC, columns=['date', 'groupC'])

        fresh_count   = fresh_count.set_index('date', drop=True)
        group_A_count = group_A_count.set_index('date', drop=True)
        group_B_count = group_B_count.set_index('date', drop=True)
        group_C_count = group_C_count.set_index('date', drop=True)

        self.all_groups_count = pd.concat((fresh_count, group_A_count, group_B_count, group_C_count), axis=1)
        
        self.fresh_ids   = pd.DataFrame.from_records(fresh_ids).set_index(0)
        self.group_A_ids = pd.DataFrame.from_records(groupA_ids).set_index(0)
        self.group_B_ids = pd.DataFrame.from_records(groupB_ids).set_index(0)
        self.group_C_ids = pd.DataFrame.from_records(groupC_ids).set_index(0)
        
        return [self.fresh_ids, self.group_A_ids, self.group_B_ids,
                self.group_C_ids, self.all_groups_count]
        
        
        
        
    
    def create_monthly (self):
        
        self.all_groups_count_monthly1 = self.all_groups_count.copy() 
        
        self.all_groups_count_monthly1['year']    = self.all_groups_count_monthly1.index.year
        self.all_groups_count_monthly1['month']   = self.all_groups_count_monthly1.index.month
        self.all_groups_count_monthly1['days']    = self.all_groups_count_monthly1.index.days_in_month
    
        
        self.all_groups_count_monthly   = self.all_groups_count_monthly1.groupby(['year', 'month', 'days']).agg(
            {'fresh' : 'mean',
             'groupA': 'mean',
             'groupB': 'mean',
             'groupC': 'mean',
             }
            ).reset_index()
        
        
        return self.all_groups_count_monthly
    
    def write_to_csv (self):
        
        self.reconcile_counts = pd.concat((self.all_groups_count, self.herd_daily), axis=1)
        

        self.reconcile_counts  .to_csv('F:\\COWS\\data\\status\\reconcile_counts.csv')        
        
        self.fresh_ids  .to_csv('F:\\COWS\\data\\status\\fresh_ids.csv')
        self.group_A_ids.to_csv('F:\\COWS\\data\\status\\group_A_ids.csv')
        self.group_B_ids.to_csv('F:\\COWS\\data\\status\\group_B_ids.csv')
        self.group_C_ids.to_csv('F:\\COWS\\data\\status\\group_C_ids.csv', na_rep='')

        self.all_groups_count_monthly  .to_csv('F:\\COWS\\data\\status\\all_groups_count_monthly.csv')        
    
if __name__ == "__main__":
    sg = statusGroups()