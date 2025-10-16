'''milk_functions.report_milk.tests.py'''
import inspect
from datetime import timedelta
import pandas as pd
from insem_functions.insem_ultra_basics import InsemUltraBasics
from feed_functions.feedcost_basics     import Feedcost_basics


class Tests:
    def __init__(self, insem_ultra_basics=None, feed_cost_basics=None):

        print(f"Tests instantiated by: {inspect.stack()[1].filename}")

        self.IUB = insem_ultra_basics or InsemUltraBasics()
        self.FCB = feed_cost_basics or Feedcost_basics()
        self.fullday = pd.read_csv("F:\\COWS\\data\\milk_data\\fullday\\fullday.csv")

        [self.date_of_change, self.last_date_before_change_feed,
            self.start_date, self.end_date ] =self.create_dates()
        
        self.test_frame     = self.create_test_frame()
        self.milking_days   = self.get_days_milking()

        [self.df1, 
         self.df2, 
         self.df3]      = self.create_doubleframe()
        self.comp       = self.create_averages()
        self.averages_by_group         = self.create_group_averages()
        self.merge_with_averages_by_group()

        self.write_to_excel()

    def create_dates(self):
        
        self.date_of_change = "2025-09-28"
        self.date_of_change = pd.to_datetime(self.date_of_change, format='%Y-%m-%d')

        self.start_date     = self.date_of_change - timedelta(days=3)
        self.end_date       = self.fullday['datex'].max()
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
        self.test_frame= liters3
        return self.test_frame
    

    def get_days_milking(self):
        iub = self.IUB.last_calf.set_index('WY_id', drop=True)
        lastcalf_bdates = iub['last calf bdate']
        md1    = (self.date_of_change - lastcalf_bdates).dt.days
        # Get cow IDs from test_frame columns as integers
        test_frame_cows = self.test_frame.columns.astype(int)
        
        # Filter md1 to only include cows that exist in test_frame
        md2 = md1[md1.index.isin(test_frame_cows)]
        self.milking_days = pd.DataFrame(md2)
        self.milking_days.columns = ['milking days']
        return self.milking_days


    def create_doubleframe(self):
        tf= self.test_frame
        tf.index = pd.to_datetime(tf.index)

        df1_daterange = pd.date_range(self.start_date, self.last_date_before_change_feed)  #exclusive of date of change
        df2_daterange = pd.date_range(self.date_of_change,self.end_date )
        df3_daterange = pd.date_range(self.date_of_change + timedelta(days=9),self.date_of_change + timedelta(days=12) )

        self.df1 = tf.loc[df1_daterange]
        self.df2 = tf.loc[df2_daterange]
        self.df3 = tf.loc[df3_daterange]        
                # Convert datetime column headers to strings
        self.df1.columns = self.df1.columns.astype(str)
        self.df2.columns = self.df2.columns.astype(str)
        self.df3.columns = self.df3.columns.astype(str)        

        return self.df1, self.df2, self.df3
    
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



        avg_df1 = df1_T.mean(axis=1)
        avg_df2 = df2_T.mean(axis=1)
        avg_df3 = df3_T.mean(axis=1)

        comp1 = avg_df1
        comp1 = pd.DataFrame({'avg before': avg_df1})        
        comp1['avg after'] = avg_df2
        comp1['liters after - before '] = comp1['avg after'] - comp1['avg before']
        comp1['% chg, after/before '] = (((comp1['avg after'] / comp1['avg before'] )-1).round(2))
        comp1['avg after 9 days'] = avg_df3
        comp1['liters after - before (recent) '] = comp1['avg after 9 days'] - comp1['avg before']
        comp1['% chg, after/before (recent)'] =( ((comp1['avg after 9 days'] / comp1['avg before'] ) -1).round(2))

        comp1 = comp1.sort_values('% chg, after/before ')
        comp1.index = comp1.index.astype(int)

        comp2 = pd.merge(comp1, self.milking_days, 
                             left_index=True, 
                             right_index=True, 
                             how='left')
        comp2.index.name = "WY_id"
        comp2.reset_index(drop='false')
        self.comp = comp2
        return self.comp
    
    
    def create_group_averages(self):
        groups = pd.read_excel("F:\\COWS\\data\\milk_data\\lactations\\tests_for_feed_change\\groups_9_28.xlsx", index_col=0)
        df1 = self.comp
        df2 = pd.merge(df1, groups['group'],  how='left', on="WY_id" )

        df3 = df2.groupby('group').agg('mean')

        self.averages_by_group = df3
        return self.averages_by_group
    
    def merge_with_averages_by_group(self):
        # fcbg=feedcostbygroup
        fcbg1 = pd.read_csv("F:\\COWS\\data\\feed_data\\feedcost_by_group\\feedcostByGroup__per_cow_daily.csv")
        fcbg2 = fcbg1.copy()
        fcbg3 = fcbg2.set_index('datex', drop=True)
        fcbg4 = pd.DataFrame(fcbg3.loc["2025-09-28",:])

        fcbg_T = fcbg4.T
        fcbg_T.columns = ['A', 'B', 'C', 'F']
        fcbg5=fcbg_T.T
        fcbg5.columns = ['orig cost']
        fcbg5.index.name = 'group'

        nfcc1 = pd.merge(self.averages_by_group, fcbg4, how='left', left_index=True, right_index=True )
        x=1






    def write_to_excel(self):

        with pd.ExcelWriter("F:\\COWS\\data\\milk_data\\lactations\\tests_for_feed_change\\sep28_2025_TEST.xlsx") as writer:
            self.comp.to_excel(writer, sheet_name='comp')
            self.averages_by_group.to_excel(writer, sheet_name='averages by group')
    
if __name__ == "__main__":
    Tests()
    
