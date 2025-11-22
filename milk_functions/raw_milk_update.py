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
        data = get_data('F:\\COWS\\data\\daily_milk.ods')

        self.AM_liters_csv = None
        self.AM_wy_csv = None
        self.PM_liters_csv = None
        self.PM_wy_csv = None
        self.group_A_csv = None
        self.group_B_csv = None
        self.group_C_csv = None
        self.group_F_csv = None        
        self.sick_csv = None

        self.amliters = None
        self.newdata_am_liters = None
        self.amwy = None
        self.newdata_AM_wy = None
        self.newdata_pm_liters = None
        self.pmwy = None
        self.pmliters = None
        self.newdata_PM_wy = None
        self.newdata_PM_liters = None
        self.group_F = None
        self.newdata_group_F = None
        self.group_A = None
        self.newdata_group_A = None
        self.group_B = None
        self.newdata_group_B = None
        self.group_C = None
        self.newdata_group_C = None
        self.sick = None
        self.newdata_sick = None

        self._load_ods_data(data)
        self._load_csv_data()
        self._calculate_dates()
        self._print_summary()
        self.compare_dates()
        self.halt_script()
        self._create_all()
        self.write_to_csv()

    def _load_ods_data(self, data):
        self.AM_liters_ods   = self.convert_to_dataframe(data['AM_liters'])
        self.AM_wy_ods       = self.convert_to_dataframe(data['AM_wy'])
        self.PM_liters_ods   = self.convert_to_dataframe(data['PM_liters'])
        self.PM_wy_ods       = self.convert_to_dataframe(data['PM_wy'])
        self.group_F_ods     = self.convert_to_dataframe(data['fresh'])
        self.group_A_ods     = self.convert_to_dataframe(data['group_A'])
        self.group_B_ods     = self.convert_to_dataframe(data['group_B'])
        self.group_C_ods     = self.convert_to_dataframe(data['group_C'])
        self.sick_ods        = self.convert_to_dataframe(data['sick'])

        self.dmAM_liters    = self.AM_liters_ods.iloc[:70, :]
        self.dmAM_wy        = self.AM_wy_ods.iloc[:70, :]
        self.dmPM_liters    = self.PM_liters_ods.iloc[:70, :]
        self.dmPM_wy        = self.PM_wy_ods.iloc[:70, :]
        self.dmgroup_F      = self.group_F_ods.iloc[:55, :]
        self.dmgroup_A      = self.group_A_ods.iloc[:55, :]
        self.dmgroup_B      = self.group_B_ods.iloc[:55, :]
        self.dmgroup_C      = self.group_C_ods.iloc[:55, :]
        self.dmsick         = self.sick_ods.iloc[:55, :]

    def _load_csv_data(self):
        csv_files = {
            'AM_liters_csv': 'F:\\COWS\\data\\milk_data\\raw\\AM_liters.csv',
            'AM_wy_csv':     'F:\\COWS\\data\\milk_data\\raw\\AM_wy.csv',
            'PM_liters_csv': 'F:\\COWS\\data\\milk_data\\raw\\PM_liters.csv',
            'PM_wy_csv':     'F:\\COWS\\data\\milk_data\\raw\\PM_wy.csv',
            'group_A_csv':   'F:\\COWS\\data\\milk_data\\wb_groups\\group_A.csv',
            'group_B_csv':   'F:\\COWS\\data\\milk_data\\wb_groups\\group_B.csv',
            'group_C_csv':   'F:\\COWS\\data\\milk_data\\wb_groups\\group_C.csv',
            'group_F_csv':   'F:\\COWS\\data\\milk_data\\wb_groups\\group_F.csv',
            'sick_csv':      'F:\\COWS\\data\\milk_data\\wb_groups\\sick.csv'
        }
        for attr, path in csv_files.items():
            df = pd.read_csv(path, index_col=0, header=0)
            df.columns = pd.to_datetime(df.columns, errors='coerce')
            setattr(self, attr, df)

    def _calculate_dates(self):
        self.csv_lastdate = pd.to_datetime(self.AM_liters_csv.columns[-1])
        self.dm_lastdate = pd.to_datetime(self.dmAM_liters.columns[-1])
        self.next_date   = pd.to_datetime(self.csv_lastdate + timedelta(days=1))
        self.gap    = (self.dm_lastdate - self.csv_lastdate)
        self.tdy = datetime.now().strftime("%Y%m%d_%H%M%S")

    def _print_summary(self):
        print(
            f"gap = {self.gap}\n"
            f"AM liters:\n{self.AM_liters_ods.iloc[:2, -3:]}\n"
            f"AM wy:\n{self.AM_wy_ods.iloc[:2, -3:]}\n"
            f"PM liters:\n{self.PM_liters_ods.iloc[:2, -3:]}\n"
            f"PM wy:\n{self.PM_wy_ods.iloc[:2, -3:]}\n"
        )

    def _create_all(self):
        self.amliters,  self.newdata_am_liters      = self.create_AM_liters()
        self.amwy,      self.newdata_AM_wy          = self.create_AM_wy()
        self.pmliters,  self.newdata_pm_liters      = self.create_PM_liters()
        self.pmwy,      self.newdata_PM_wy          = self.create_PM_wy()
        self.group_F,   self.newdata_group_F        = self.create_group_F()        
        self.group_A,   self.newdata_group_A        = self.create_group_A()
        self.group_B,   self.newdata_group_B        = self.create_group_B()
        self.group_C,   self.newdata_group_C        = self.create_group_C() 
        self.sick,      self.newdata_sick           = self.create_sick()


    def convert_to_dataframe(self, sheet_data):
        df = pd.DataFrame(sheet_data)
        df = df.iloc[2:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df.set_index(df.columns[0], inplace=True)
        df.index = df.index.fillna(0)
        df.index = df.index.astype(int)
        df.columns = pd.to_datetime(df.columns, errors='coerce')
        return df
        
    
    def compare_dates(self):
        print('last date in daily milk = ',self.dm_lastdate, 'last date in AM liters = ',self.csv_lastdate)    

        
    def halt_script(self):
        if self.dm_lastdate <= self.csv_lastdate:
            print('date not Ok, halting', self.dm_lastdate, self.csv_lastdate)
            raise HaltScriptException('Halting current module execution')
            
        elif self.dm_lastdate > self.csv_lastdate:
            print('date Ok, proceeding', 'start= ', self.csv_lastdate, 'end= ', self.dm_lastdate)
         
    
    
    def create_AM_liters(self):
        self.newdata_am_liters = self.dmAM_liters.loc[:, self.next_date:self.dm_lastdate].copy()
        self.amliters=pd.concat([self.AM_liters_csv, self.newdata_am_liters],axis=1,join='inner')
        return self.amliters, self.newdata_am_liters

    def create_AM_wy(self):
        self.newdata_AM_wy = self.dmAM_wy.loc[:,self.next_date:self.dm_lastdate].copy()
        self.amwy=pd.concat([self.AM_wy_csv, self.newdata_AM_wy],axis=1,join='inner')
        return self.amwy, self.newdata_AM_wy
        
    def create_PM_liters(self):
        self.newdata_PM_liters = self.dmPM_liters.loc[:,self.next_date:self.dm_lastdate].copy()
        self.pmliters = pd.concat([self.PM_liters_csv,self.newdata_PM_liters],axis=1,join='inner')
        return self.pmliters, self.newdata_PM_liters
    
    def create_PM_wy(self):
        self.newdata_PM_wy = self.dmPM_wy.loc[:,self.next_date:self.dm_lastdate].copy()
        self.pmwy=pd.concat([self.PM_wy_csv, self.newdata_PM_wy],axis=1,join='inner')
        return self.pmwy, self.newdata_PM_wy
    
    def create_group_F(self):
        self.newdata_group_F = self.dmgroup_F.loc[:,self.next_date:self.dm_lastdate].copy()
        self.group_F=pd.concat([self.group_F_csv, self.newdata_group_F],axis=1,join='inner')
        return self.group_F, self.newdata_group_F

    def create_group_A(self):
        self.newdata_group_A = self.dmgroup_A.loc[:,self.next_date:self.dm_lastdate].copy()
        self.group_A=pd.concat([self.group_A_csv, self.newdata_group_A],axis=1,join='inner')
        return self.group_A, self.newdata_group_A

    def create_group_B(self):
        self.newdata_group_B = self.dmgroup_B.loc[:,self.next_date:self.dm_lastdate].copy()
        self.group_B=pd.concat([self.group_B_csv, self.newdata_group_B],axis=1,join='inner')
        return self.group_B, self.newdata_group_B

    def create_group_C(self):
        self.newdata_group_C = self.dmgroup_C.loc[:,self.next_date:self.dm_lastdate].copy()
        self.group_C =pd.concat([self.group_C_csv, self.newdata_group_C],axis=1,join='inner')
        return self.group_C, self.newdata_group_C

    def create_sick(self):
        self.newdata_sick = self.dmsick.loc[:,self.next_date:self.dm_lastdate].copy()
        self.sick =pd.concat([self.sick_csv, self.newdata_sick],axis=1,join='inner')
        return self.sick, self.newdata_sick               

    #write to file using numeric date format
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
        


if __name__ =="__main__":
    obj=RawMilkUpdate()