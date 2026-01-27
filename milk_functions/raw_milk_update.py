'''
rawmilkupdate.py
'''

from datetime import datetime, timedelta
import pandas as pd  
from pyexcel_ods import get_data

class HaltScriptException(Exception):
    pass
class RawMilkUpdate:

    def __init__(self):
        self.sheet_names = [
            'AM_liters', 'AM_wy', 'PM_liters', 'PM_wy',
            'fresh', 'group_A', 'group_B', 'group_C', 'sick'
        ]
        self.attr_map = {
            'AM_liters': 'AM_liters',
            'AM_wy': 'AM_wy',
            'PM_liters': 'PM_liters',
            'PM_wy': 'PM_wy',
            'fresh': 'group_F',
            'group_A': 'group_A',
            'group_B': 'group_B',
            'group_C': 'group_C',
            'sick': 'sick',
        }

        # Initialize all instance attributes to None for Pylint and clarity
        self.AM_liters = None
        self.AM_wy = None
        self.PM_liters = None
        self.PM_wy = None
        self.group_F = None
        self.group_A = None
        self.group_B = None
        self.group_C = None
        self.sick = None
        self.xls_data_renamed = {}

        self.AM_liters_csv = None
        self.AM_wy_csv = None
        self.PM_liters_csv = None
        self.PM_wy_csv = None

        self.amliters = None
        self.newdata_am_liters = None
        self.amwy = None
        self.newdata_AM_wy = None
        self.newdata_pm_liters = None
        self.pmwy = None
        self.pmliters = None
        self.newdata_PM_wy = None
        self.newdata_PM_liters = None
        self.newdata_group_F = None
        self.newdata_group_A = None
        self.newdata_group_B = None
        self.newdata_group_C = None
        self.newdata_sick = None

        self.attr_map = {}

        self.csv_lastdate = None
        self.dm_lastdate = None
        self.next_date = None
        self.gap = None
        self.tdy = None

        self.file_path = None
        self.processed_data_dict = {}

        self.load_and_process()
        self.write_to_csv()


    def load_and_process(self):
        self.load_data()
        self.process_data()

    def load_data(self):
        # Load xlsx data
        self.file_path = r"F:\COWS\data\milk_data\daily_milk\daily_milk.xlsx"
        attr_map = {
            'AM_liters': 'AM_liters',
            'AM_wy': 'AM_wy',
            'PM_liters': 'PM_liters',
            'PM_wy': 'PM_wy',
            'fresh': 'group_F',
            'group_A': 'group_A',
            'group_B': 'group_B',
            'group_C': 'group_C',
            'sick': 'sick',
        }

        # this returns a dict; standard pandas behavior when sheet_name is a list.
        xls_data = pd.read_excel(self.file_path, sheet_name=self.sheet_names, header=0)
        # Rename xls_data DataFrames to xxxxx_xls and store as instance attribute
        self.xls_data_renamed = {}
        for sheet, df in xls_data.items():
            # Clean the DataFrame: set row 1 as header, slice from row 2 onward
            df = df.reset_index(drop=True)
            df.columns = df.iloc[1]
            df_cleaned = df.iloc[2:70, :]
            self.xls_data_renamed[f"{sheet}_xls"] = df_cleaned
            # Also assign to attribute if in attr_map
            if sheet in attr_map:
                setattr(self, attr_map[sheet], df_cleaned)
        
        # Load  the csv files as df
        csv_files = {
            'AM_liters_csv': r'F:\COWS\data\milk_data\raw\AM_liters.csv',
            'AM_wy_csv':     r'F:\COWS\data\milk_data\raw\AM_wy.csv',
            'PM_liters_csv': r'F:\COWS\data\milk_data\raw\PM_liters.csv',
            'PM_wy_csv':     r'F:\COWS\data\milk_data\raw\PM_wy.csv',
        }
        for attr, path in csv_files.items():
            df = pd.read_csv(path, index_col=0, header=0)
            df.columns = pd.to_datetime(df.columns, errors='coerce')
            setattr(self, attr, df)

        # Move date calculations and summary printing here
        self._calculate_dates()
        self._print_summary()
        self.compare_dates()

        self.halt_script()


    def _calculate_dates(self):
        self.csv_lastdate = pd.to_datetime(self.AM_liters_csv.columns[-1])
        # Always get AM_liters_xls from xls_data_renamed
        am_liters_xls = self.xls_data_renamed['AM_liters_xls']
        self.dm_lastdate = pd.to_datetime(am_liters_xls.columns[-1])
        self.next_date   = pd.to_datetime(self.csv_lastdate + timedelta(days=1))
        self.gap    = (self.dm_lastdate - self.csv_lastdate)
        self.tdy = datetime.now().strftime("%Y%m%d_%H%M%S")

    def _print_summary(self):
        print(
            f"gap = {self.gap}\n"
            f"AM liters:\n{self.AM_liters.iloc[:2, -3:]}\n"
            f"AM wy:\n{self.AM_wy.iloc[:2, -3:]}\n"
            f"PM liters:\n{self.PM_liters.iloc[:2, -3:]}\n"
            f"PM wy:\n{self.PM_wy.iloc[:2, -3:]}\n"
        )

    def compare_dates(self):
        print('last date in daily milk = ',self.dm_lastdate, 'last date in AM liters = ',self.csv_lastdate)    

        
    def halt_script(self):
        if self.dm_lastdate <= self.csv_lastdate:
            print('date not Ok, halting', self.dm_lastdate, self.csv_lastdate)
            raise HaltScriptException('Halting current module execution')
            
        elif self.dm_lastdate > self.csv_lastdate:
            print('date Ok, proceeding', 'start= ', self.csv_lastdate, 'end= ', self.dm_lastdate)
         


    def process_data(self):
        # No slicing needed; DataFrames are already sliced in load_data
        # List of tuples: (dm_attr, csv_attr, newdata_attr, out_attr)
        attr_sets = [
            ('AM_liters_xls', 'AM_liters_csv', 'newdata_am_liters', 'amliters'),
            ('AM_wy_xls', 'AM_wy_csv', 'newdata_AM_wy', 'amwy'),
            ('PM_liters_xls', 'PM_liters_csv', 'newdata_PM_liters', 'pmliters'),
            ('PM_wy_xls', 'PM_wy_csv', 'newdata_PM_wy', 'pmwy'),
            ('group_F_xls', 'group_F_csv', 'newdata_group_F', 'group_F'),
            ('group_A_xls', 'group_A_csv', 'newdata_group_A', 'group_A'),
            ('group_B_xls', 'group_B_csv', 'newdata_group_B', 'group_B'),
            ('group_C_xls', 'group_C_csv', 'newdata_group_C', 'group_C'),
            ('sick_xls', 'sick_csv', 'newdata_sick', 'sick'),
        ]
        self.processed_data_dict = {}
        for xls_key, csv_attr, newdata_attr, out_attr in attr_sets:
            dm = self.xls_data_renamed.get(xls_key, None)
            csv = getattr(self, csv_attr, None)
            if dm is not None and csv is not None:
                newdata = dm.loc[:, self.next_date:self.dm_lastdate].copy()
                setattr(self, newdata_attr, newdata)
                out = pd.concat([csv, newdata], axis=1, join='inner')
                setattr(self, out_attr, out)
                self.processed_data_dict[out_attr] = out



    def write_to_csv(self):
        

        self.amliters   .to_csv(f"D:\\Cows\\data_backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        self.amliters   .to_csv(f"E:\\Cows\\data_backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_{self.tdy}.csv")
        self.amliters   .to_csv('F:\\COWS\\data\\milk_data\\raw\\AM_liters.csv',mode='w',header=True,index=True)
        
        self.amwy       .to_csv(f"D:\\Cows\\data_backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        self.amwy       .to_csv(f"E:\\Cows\\data_backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_{self.tdy}.csv")
        self.amwy       .to_csv( 'F:\\COWS\\data\\milk_data\\raw\\AM_wy.csv',mode='w',header=True,index=True)
        
        self.pmliters   .to_csv(f"D:\\Cows\\data_backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        self.pmliters   .to_csv(f"E:\\Cows\\data_backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_{self.tdy}.csv")
        self.pmliters   .to_csv( 'F:\\COWS\\data\\milk_data\\raw\\PM_liters.csv',mode='w',header=True,index=True)
        
        self.pmwy       .to_csv(f"D:\\Cows\\data_backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        self.pmwy       .to_csv(f"E:\\Cows\\data_backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_{self.tdy}.csv")
        self.pmwy       .to_csv( 'F:\\COWS\\data\\milk_data\\raw\\PM_wy.csv',mode='w',header=True,index=True)


        # groups backup
        self.group_A    .to_csv(f"D:\\Cows\\data_backup\\milk backup\\wb_groups\\group_A\\group_A{self.tdy}.csv")
        self.group_A    .to_csv(f"E:\\Cows\\data_backup\\milk backup\\wb_groups\\group_A\\group_A{self.tdy}.csv")
        self.group_A    .to_csv( 'F:\\COWS\\data\\milk_data\\wb_groups\\group_A.csv',mode='w',header=True,index=True)

        self.group_B    .to_csv(f"D:\\Cows\\data_backup\\milk backup\\wb_groups\\group_B\\group_B{self.tdy}.csv")
        self.group_B    .to_csv(f"E:\\Cows\\data_backup\\milk backup\\wb_groups\\group_B\\group_B{self.tdy}.csv")
        self.group_B    .to_csv( 'F:\\COWS\\data\\milk_data\\wb_groups\\group_B.csv',mode='w',header=True,index=True)

        self.group_C    .to_csv(f"D:\\Cows\\data_backup\\milk backup\\wb_groups\\group_C\\group_C{self.tdy}.csv")
        self.group_C    .to_csv(f"E:\\Cows\\data_backup\\milk backup\\wb_groups\\group_C\\group_C{self.tdy}.csv")
        self.group_C    .to_csv( 'F:\\COWS\\data\\milk_data\\wb_groups\\group_C.csv',mode='w',header=True,index=True)
        
        self.group_F    .to_csv(f"D:\\Cows\\data_backup\\milk backup\\wb_groups\\group_F\\fresh{self.tdy}.csv")
        self.group_F    .to_csv(f"E:\\Cows\\data_backup\\milk backup\\wb_groups\\group_F\\fresh{self.tdy}.csv")
        self.group_F    .to_csv( 'F:\\COWS\\data\\milk_data\\wb_groups\\group_F.csv',mode='w',header=True,index=True)
        
        self.sick    .to_csv(f"D:\\Cows\\data_backup\\milk backup\\wb_groups\\sick\\fresh{self.tdy}.csv")
        self.sick    .to_csv(f"E:\\Cows\\data_backup\\milk backup\\wb_groups\\sick\\fresh{self.tdy}.csv")
        self.sick    .to_csv( 'F:\\COWS\\data\\milk_data\\wb_groups\\sick.csv',mode='w',header=True,index=True)

if __name__ =="__main__":
    obj=RawMilkUpdate()