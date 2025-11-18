'''milk_functions\\model_groups.py'''
import inspect
import pandas as pd

from container import get_dependency


class ModelGroups:
    def __init__(self):

        print(f"ModelGroups instantiated by: {inspect.stack()[1].filename}")

        self.SD = None
        self.WD = None
        # self.MG = None
        self.IUB = None
        self.MB = None
        self.DR = None
        self.DRM = None
        self.startdate = None
        self.herd_daily = None
        self.whiteboard_groups = None
        self.fresh_ids = None
        self.group_A_ids = None
        self.group_B_ids = None
        self.group_C_ids = None
        self.all_groups_count = None
        self.model_groups_lastrow = None
        self.all_groups_count_monthly = None

    def load_and_process(self):

        self.SD = get_dependency('status_data')
        self.WD = get_dependency('wet_dry')
        self.IUB= get_dependency('insem_ultra_basics')
        self.MB = get_dependency('milk_basics')
        self.DR = get_dependency('date_range')
        
        self.DRM = self.DR.date_range_monthly_data
        self.startdate = self.DR.startdate
        self.herd_daily = self.SD.herd_daily

        [self.fresh_ids, self.group_A_ids, 
         self.group_B_ids, self.group_C_ids, 
         self.all_groups_count]     = self.create_model_groups()
        
        self.model_groups_lastrow   = self.get_model_groups_lastrow()

        self.all_groups_count_monthly = self.create_monthly()
        self.write_to_csv()
        

    def create_model_groups (self):

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
            j1 = 0
            
            for i in wet.columns:
                j1 = wet.loc[date1, i]
                m1 = milk1.loc[date1, str(i)]
                
                if not pd.isna(m1):
                    m1 = milk1[str(i)].rolling(window=7).mean().loc[date1] #7 day MAvg
                    days1 = j1
                    # j = int(j1) - 1

                    if 0 < days1<= 14 :
                        freshx += 1
                        F_ids = i

                    elif (days1 >14 ) and m1 >= 15:   #140 days=20wks
                        groupAx += 1
                        A_ids = i

                    elif (days1 > 14 ) and (m1 < 15 and m1 >=9):
                        groupBx += 1
                        B_ids = i

                    elif days1 > 14  and m1 < 9:
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
        self.all_groups_count['total count'] = self.all_groups_count.sum(axis=1)
        
        self.fresh_ids   = pd.DataFrame.from_records(fresh_ids).set_index(0)
        self.group_A_ids = pd.DataFrame.from_records(groupA_ids).set_index(0)
        self.group_B_ids = pd.DataFrame.from_records(groupB_ids).set_index(0)
        self.group_C_ids = pd.DataFrame.from_records(groupC_ids).set_index(0)
        
        return [self.fresh_ids, self.group_A_ids, self.group_B_ids,
                self.group_C_ids, self.all_groups_count]
        
        
    def get_model_groups_lastrow(self):
        f1=self.fresh_ids.iloc[-1:,:].copy()
        a1=self.group_A_ids.iloc[-1:,:].copy()
        b1=self.group_B_ids.iloc[-1:,:].copy()
        c1=self.group_C_ids.iloc[-1:,:].copy()

        f=f1.T
        f.columns = ["WY_id"]
        f['group'] = "F"

        
        a=a1.T
        a.columns = ["WY_id"]
        a['group'] = "A"

        
        b=b1.T
        b.columns = ["WY_id"]
        b['group'] = "B"

        
        c=c1.T
        c.columns = ["WY_id"]
        c['group'] = "C"

        lds1 = pd.concat([f,a,b,c], axis=0)
        lds2 = lds1.sort_values(by="WY_id")
        lds = lds2.dropna(subset=['WY_id']).reset_index(drop=True)

        self.model_groups_lastrow =lds
        return     self.model_groups_lastrow   
        

    def create_monthly (self):
        
        all_groups_count_monthly1 = self.all_groups_count.copy() 
        
        all_groups_count_monthly1['year']    = all_groups_count_monthly1.index.year
        all_groups_count_monthly1['month']   = all_groups_count_monthly1.index.month
        all_groups_count_monthly1['days']    = all_groups_count_monthly1.index.days_in_month
    
        self.all_groups_count_monthly   = all_groups_count_monthly1.groupby(['year', 'month', 'days']).agg(
            {'fresh' : 'mean',
             'groupA': 'mean',
             'groupB': 'mean',
             'groupC': 'mean',
             }
            ).reset_index()
        
        
        return self.all_groups_count_monthly
    
    def write_to_csv (self):
        
        reconcile_counts = pd.concat((self.all_groups_count, self.herd_daily), axis=1)

        reconcile_counts.to_csv('F:\\COWS\\data\\status\\reconcile_counts.csv')        
        self.fresh_ids  .to_csv('F:\\COWS\\data\\status\\fresh_ids_model.csv')
        self.group_A_ids.to_csv('F:\\COWS\\data\\status\\group_A_ids_model.csv')
        self.group_B_ids.to_csv('F:\\COWS\\data\\status\\group_B_ids_model.csv')
        self.group_C_ids.to_csv('F:\\COWS\\data\\status\\group_C_ids_model.csv')

        self.all_groups_count_monthly  .to_csv('F:\\COWS\\data\\status\\all_groups_count_monthly.csv')        
    
if __name__ == "__main__":
    model_groups = ModelGroups()
    model_groups.load_and_process()    