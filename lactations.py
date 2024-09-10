'''lactations.py'''

import pandas as pd    #123   456
import numpy as np
from wet_dry        import WetDry
from insem_ultra    import InsemUltraData
# from status         import StatusData
# from status_module_adjustment import StatusModuleAdjustment


class Lactations:
    ''' asdfasdf'''
    def __init__(self):

        self.start  = pd.read_csv    ('F:\\COWS\\data\\csv_files\\live_births.csv',     header = 0, parse_dates = ['b_date'])
        self.stop   = pd.read_csv    ('F:\\COWS\\data\\csv_files\\stop_dates.csv',      header = 0, parse_dates = ['stop'])
        self.milk   = pd.read_csv    ('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv', header = 0, index_col   = 'datex', parse_dates=['datex'])
        self.bd     = pd.read_csv    ('F:\\COWS\\data\\csv_files\\birth_death.csv',     header = 0, parse_dates = ['birth_date','death_date'])

        # self.ma     = StatusModuleAdjustment()
        # self.sd     = StatusData()
        self.iud    = InsemUltraData()
        self.wd     = WetDry()

        self.max_milking_cownum  = self.milk.T.index.max()    # no heifers
        self.max_bd_cownum       = self.bd.index.max()        # including heifers

        # functions
        self.lact4               = self.create_lactations()
        self.lact_1              = self.create_lactation_1()
        self.lact_2              = self.create_lactation_2()
        self.lact_3              = self.create_lactation_3()
        self.lact_4              = self.create_lactation_4()
        self.lact_5              = self.create_lactation_5()
        self.create_write_to_csv()

    def create_lactations(self):
        milk_array = self.wd.milk_array
        rows = list( self.wd.stop_1.index)      #integers
        cols = self.wd.start_1.columns  #integers decremented by 1

        lact2, lact3, lact4 = [],[],[]

        for j in rows:
            for i in cols:
                start       = self.wd.start_1 .loc[j,i]
                stop        = self.wd.stop_1  .loc[j,i]

                lact1 = milk_array.loc[start:stop,[str(i)]]
                lact1.reset_index(drop=True, inplace=True)
                lact2.append(lact1)
            lact3.append(lact2)
            lact2 = []
        lact4.append(lact3)
        lact3=[]
        return lact4


    def create_lactation_1(self):
        lact, lact_1, subarray = [],[],[]
        max_len = 2000
        subarray1 = self.lact4[0][0]

        for df in subarray1:
            i= df.iloc[:,0]
            sf = pd.to_numeric(i, errors='coerce')
            subarray.append(sf)

        for i in subarray:
            pad = max_len - len(i)
            xx = np.pad(i,(0,pad),'constant')
            lact.append(xx)
        lact_1x = np.vstack(lact)
        lact_1 = pd.DataFrame(lact_1x)

        return     lact_1



    def create_lactation_2(self):
        lact, lact_2, subarray = [],[],[]
        max_len = 2000
        subarray1 = self.lact4[0][1]

        for df in subarray1:
            i= df.iloc[:,0]
            sf = pd.to_numeric(i, errors='coerce')
            subarray.append(sf)

        for i in subarray:
            pad = max_len - len(i)
            xx = np.pad(i,(0,pad),'constant')
            lact.append(xx)

        lact_2x = np.vstack(lact)
        lact_2 = pd.DataFrame(lact_2x)

        return     lact_2




    def create_lactation_3(self):
        lact, lact_3, subarray = [],[],[]
        max_len = 2000
        subarray1 = self.lact4[0][2]


        for df in subarray1:
            i= df.iloc[:,0]
            sf = pd.to_numeric(i, errors='coerce')
            subarray.append(sf)

        for i in subarray:
            pad = max_len - len(i)
            xx = np.pad(i,(0,pad),'constant')
            lact.append(xx)


        lact_3x = np.vstack(lact)
        lact_3 = pd.DataFrame(lact_3x)

        return     lact_3


    def create_lactation_4(self):
        lact, lact_4, subarray = [],[],[]
        max_len = 2000
        subarray1 = self.lact4[0][3]

        for df in subarray1:
            i= df.iloc[:,0]
            sf = pd.to_numeric(i, errors='coerce')
            subarray.append(sf)

        for i in subarray:
            pad = max_len - len(i)
            xx = np.pad(i,(0,pad),'constant')
            lact.append(xx)


        lact_4x = np.vstack(lact)
        lact_4 = pd.DataFrame(lact_4x)

        return     lact_4



    def create_lactation_5(self):
        lact, lact_5, subarray = [],[],[]
        max_len = 2000
        subarray1 = self.lact4[0][4]


        for df in subarray1:
            i= df.iloc[:,0]
            sf = pd.to_numeric(i, errors='coerce')
            subarray.append(sf)

        for i in subarray:
            pad = max_len - len(i)
            xx = np.pad(i,(0,pad),'constant')
            lact.append(xx)


        lact_5x = np.vstack(lact)
        lact_5 = pd.DataFrame(lact_5x)

        return lact_5

    def create_lactation_6(self):
        lact, lact_6, subarray = [],[],[]
        max_len = 2000
        subarray1 = self.lact_5[0][5]

        for df in subarray1:
            i= df.iloc[:,0]
            sf = pd.to_numeric(i, errors='coerce')
            subarray.append(sf)

        for i in subarray:
            pad = max_len - len(i)
            xx = np.pad(i,(0,pad),'constant')
            lact.append(xx)


            lact_6x = np.vstack(lact)
            lact_6 = pd.DataFrame(lact_6x)

            return     lact_6



    def create_write_to_csv(self):

        self.lact_1.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_1.csv')
        self.lact_2.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_2.csv')
        self.lact_3.T.to_csv('F:\\COWS\\data\\milk_data\\lactations\\lact_3.csv')
        return None
