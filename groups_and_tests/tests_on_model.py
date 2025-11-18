'''milk_functions.report_milk.Tests_on_model.py'''
import inspect
from datetime import timedelta
import pandas as pd
from container import get_dependency


class Tests_on_model:
    def __init__(self):
        print(f"Tests_on_model instantiated by: {inspect.stack()[1].filename}")
        self.IUB = None
        self.FCB = None
        self.MG = None
        self.fullday = None
        self.date_of_change = None
        self.date_of_change1 = None
        self.last_date_before_change_feed = None
        self.start_date = None
        self.end_date = None
        self.testframe = None
        self.groups_on_dayofchange = None
        self.sick_cows = []
        self.milking_days = None
        self.df1 = self.df2 = self.df3 = self.df4 = None
        self.comp = None
        self.averages_by_group = None
        self.all_cows_avgs = None
        self.gross_profit_comp = None
        self.new_feedcost = None

    def load_and_process(self):
        self.IUB = get_dependency('insem_ultra_basics')
        self.FCB = get_dependency('feedcost_basics')
        self.MG  = get_dependency('model_groups')
        self.fullday = pd.read_csv("F:\\COWS\\data\\milk_data\\fullday\\fullday.csv")
        [self.date_of_change, 
         self.last_date_before_change_feed,
         self.start_date, self.end_date] = self.create_dates()
        self.testframe = self.create_test_frame()
        self.groups_on_dayofchange = self.get_groups_for_test_day()

        self.sick_cows = [239, 267, 125, 260, 257, 268]

        self.milking_days = self.get_days_milking()
        [self.df1, self.df2, self.df3, self.df4] = self.create_doubleframe()
        [self.comp, self.averages_by_group, self.all_cows_avgs] = self.create_averages()
        self.gross_profit_comp, self.new_feedcost = self.merge_feedcost_with_averages_by_group()
        self.write_to_excel()

    def create_dates(self):
        
        self.date_of_change1 = "2025-09-27"  #NOTE  
        self.date_of_change = pd.to_datetime(self.date_of_change1, format='%Y-%m-%d')

        self.start_date     = self.date_of_change - timedelta(days=3)
        self.end_date       = '2025-10-19'
        self.last_date_before_change_feed = self.date_of_change - timedelta( days=1 )

        self.start_date     = pd.to_datetime(self.start_date,   format='%Y-%m-%d')
        self.end_date       = pd.to_datetime(self.end_date,     format='%Y-%m-%d')

        return [self.date_of_change, self.last_date_before_change_feed,
            self.start_date, self.end_date ]
    


    def create_test_frame(self):
        liters1 = self.fullday.set_index('datex', drop=True)
        liters1.index = pd.to_datetime(liters1.index, format='%Y-%m-%d')
        liters2 = liters1.loc[self.start_date:self.end_date].copy()
        #eliminate cows with any missing data - probably born then, or sick
        liters3 = liters2.dropna(axis=1).copy()

        self.testframe= liters3
        return self.testframe
    
    
    def get_groups_for_test_day(self):

        chg = self.date_of_change
        sick_set = set(self.sick_cows)

        f1=self.MG.fresh_ids   .loc[chg]
        f1 = f1[~f1.isin(sick_set)].copy()

        a1 = self.MG.group_A_ids.loc[chg]
        a1 = a1[~a1.isin(sick_set)].copy()

        b1 = self.MG.group_B_ids.loc[chg]
        b1 = b1[~b1.isin(sick_set)].copy()

        c1 = self.MG.group_C_ids.loc[chg]
        c1 = c1[~c1.isin(sick_set)].copy()

        f=f1.T.to_frame(name="WY_id")
        f['group'] = "F"

        a=a1.T.to_frame(name="WY_id")
        a['group'] = "A"

        b=b1.T.to_frame(name="WY_id")
        b['group'] = "B"
        
        c=c1.T.to_frame(name="WY_id")
        c['group'] = "C"

        x1 = pd.concat([f,a,b,c], axis=0)
        x2 = x1.sort_values("WY_id")
        x3 = x2.dropna(subset=['WY_id']).reset_index(drop=True)

        x4 = x3.set_index('WY_id', drop=True)

        self.groups_on_dayofchange = x4

        return     self.groups_on_dayofchange
    
    

    def get_days_milking(self):
        iub = self.IUB.last_calf.set_index('WY_id', drop=True)
        lastcalf_bdates = iub['last calf bdate']

        #get the number of days milking as of the date of change
        md1    = (self.date_of_change - lastcalf_bdates).dt.days
        # Get cow IDs from testframe columns as integers
        test_frame_cows = self.testframe.columns.astype(int)
        
        # Filter md1 to only include cows that exist in testframe
        md2 = md1[md1.index.isin(test_frame_cows)]
        self.milking_days = pd.DataFrame(md2)
        self.milking_days.columns = ['milking days']
        return self.milking_days


    def create_doubleframe(self):
        tf= self.testframe
        tf.index = pd.to_datetime(tf.index)

        df1_daterange = pd.date_range(self.start_date, self.last_date_before_change_feed)  #exclusive of date of change
        df2_daterange = pd.date_range(self.date_of_change + timedelta(days=1),self.date_of_change + timedelta(days=3) )
        df3_daterange = pd.date_range(self.date_of_change + timedelta(days=9),self.date_of_change + timedelta(days=11) )        
        df4_daterange = pd.date_range(self.date_of_change + timedelta(days=19),self.date_of_change + timedelta(days=21) ) 

        self.df1 = tf.loc[df1_daterange]
        self.df2 = tf.loc[df2_daterange]
        self.df3 = tf.loc[df3_daterange]
        self.df4 = tf.loc[df4_daterange]                   
                # Convert datetime column headers to strings
        self.df1.columns = self.df1.columns.astype(str)
        self.df2.columns = self.df2.columns.astype(str)
        self.df3.columns = self.df3.columns.astype(str)
        self.df4.columns = self.df4.columns.astype(str)                 

        return self.df1, self.df2, self.df3, self.df4
    
    def create_averages(self):
        # Convert datetime index to string before transposing to avoid datetime columns
        df1_copy = self.df1.copy()
        df1_copy.index = df1_copy.index.strftime('%Y-%m-%d')
        df1_T = df1_copy.T
        df1_T = df1_T.astype(float)

        
        df2_copy = self.df2.copy()
        df2_copy.index = df2_copy.index.strftime('%Y-%m-%d')
        df2_T = df2_copy.T
        df2_T = df2_T.astype(float)

        df3_copy = self.df3.copy()
        df3_copy.index = df3_copy.index.strftime('%Y-%m-%d')
        df3_T = df3_copy.T
        df3_T = df3_T.astype(float)

        df4_copy = self.df4.copy()
        df4_copy.index = df4_copy.index.strftime('%Y-%m-%d')
        df4_T = df4_copy.T
        df4_T = df4_T.astype(float)



        avg_df1 = df1_T.mean(axis=1)
        avg_df2 = df2_T.mean(axis=1)
        avg_df3 = df3_T.mean(axis=1)
        avg_df4 = df4_T.mean(axis=1)        


        #get df of the individual cows avgs
        all_cows_avgs1 = pd.concat([avg_df1, avg_df2, avg_df3, avg_df4], axis=1)
        all_cows_avgs1.index = all_cows_avgs1.index.astype(float)

        all_cows_avgs2 = pd.merge(all_cows_avgs1, self.groups_on_dayofchange,
                    left_index=True,
                    right_index=True,
                    how='left')
      
        all_cows_avgs2.index = all_cows_avgs2.index.astype(int).astype(str)
        all_cows_avgs2.index.name = 'WY_id'

        all_cows_avgs4 = all_cows_avgs2.rename(columns={0:'before'})

        all_cows_avgs4['% chg 1'] = (all_cows_avgs4[1]/all_cows_avgs4['before']) - 1
        all_cows_avgs4['% chg 2'] = (all_cows_avgs4[2]/all_cows_avgs4['before']) - 1
        all_cows_avgs4['% chg 3'] = (all_cows_avgs4[3]/all_cows_avgs4['before']) - 1

        all_cows_avgs4 = all_cows_avgs4.sort_values(['group','% chg 3'], ascending=[True, False])


        comp1 = avg_df1 #avg for 3 days prior to date of change (inclusive)
        comp1 = pd.DataFrame({'avg before': avg_df1})        
        comp1['avg 3d']             = avg_df2       #avg for 3 days following date of change
        comp1['liters 3d diff  ']   = comp1['avg 3d'] - comp1['avg before']   #diff in liters - first 3days
        comp1['% chg 3d ']          = (((comp1['avg 3d'] / comp1['avg before'] )-1).round(2))    # % diff
        comp1['avg after 10d']      = avg_df3
        comp1['liters 10d diff']    = comp1['avg after 10d'] - comp1['avg before']
        comp1['% chg 10d']          =( ((comp1['avg after 10d'] / comp1['avg before'] ) -1).round(2))
        comp1['avg after 20d']      = avg_df4
        comp1['liters 20d diff']    = comp1['avg after 20d'] - comp1['avg before']
        comp1['% chg 20d']          =( ((comp1['avg after 20d'] / comp1['avg before'] ) -1).round(2))        

        comp1 = comp1.sort_values('% chg 20d')
        comp1.index = comp1.index.astype(int)

        comp2 = pd.merge(comp1, self.milking_days, 
                             left_index=True, 
                             right_index=True, 
                             how='left')
        comp2.index.name = "WY_id"
        comp2.reset_index(drop='false')

        comp3 = pd.merge(comp2, self.groups_on_dayofchange,
                         left_index=True,
                         right_index=True,
                         how='left')
        self.comp = comp3
        
        comp4 = comp3.groupby('group').agg('mean')
        self.averages_by_group = comp4
        self.all_cows_avgs = all_cows_avgs4

        return self.comp, self.averages_by_group, self.all_cows_avgs
    
    def merge_feedcost_with_averages_by_group(self):
        # fcbg=feedcostbygroup
        fcbg1 = pd.read_csv("F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup__per_cow_daily.csv")
        fcbg2 = fcbg1.copy()
        fcbg3 = fcbg2.set_index('datex', drop=True)
        
        fcbg3.index = pd.to_datetime(fcbg3.index, format='%Y-%m-%d')
        fcbg4 = pd.DataFrame(fcbg3.loc[self.last_date_before_change_feed:self.date_of_change ])
        
        fcbg_T = fcbg4.T
        fcbg_T.columns = ['orig cost', 'new cost']

        fcbg5=fcbg_T.T
        fcbg5.columns = ['A', 'B', 'C', 'F']   
        fcbg6 = fcbg5.T     
  

        avg_liters_by_group = self.averages_by_group[['avg before','avg 3d','avg after 10d','avg after 20d' ]]

        nfcc1 = pd.merge(avg_liters_by_group, fcbg6, how='left', left_index=True, right_index=True )
       
        milkprice=22
        nfcc1['rev before'] = nfcc1['avg before']    * milkprice
        nfcc1['rev 3d']     = nfcc1['avg 3d']        * milkprice
        nfcc1['rev 10d']    = nfcc1['avg after 10d'] * milkprice
        nfcc1['rev 20d']    = nfcc1['avg after 20d'] * milkprice

        nfcc1['GP before']  = nfcc1['rev before'] - nfcc1['orig cost']
        nfcc1['GP 3d']      = nfcc1['rev 3d']  - nfcc1['new cost']
        nfcc1['GP 10d']     = nfcc1['rev 10d'] - nfcc1['new cost']
        nfcc1['GP 20d']     = nfcc1['rev 20d'] - nfcc1['new cost']                

        self.gross_profit_comp = nfcc1.T
        self.new_feedcost = fcbg6


        return self.gross_profit_comp, self.new_feedcost 
    

    def write_to_excel(self):
        abg = self.averages_by_group.T
        with pd.ExcelWriter(f"F:\\COWS\\data\\milk_data\\tests\\test_on_model{self.date_of_change1}.xlsx") as writer:
            self.comp.to_excel(writer,              sheet_name='comp')
            abg.to_excel(writer,                    sheet_name='averages by group')
            self.gross_profit_comp.to_excel(writer, sheet_name='GP comp')
            self.all_cows_avgs.to_excel(writer,     sheet_name='all cows avgs')
    
if __name__ == "__main__":
    obj=Tests_on_model()
    obj.load_and_process()     
