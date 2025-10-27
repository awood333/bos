'''RunInsemDataBackup.py'''

import pandas as pd 

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class InsemDataBackup:
    def __init__(self):
        
        self.bd, self.lb, self.i, self.sd, self.u  = self.read_csv_data()
        self.write_to_csv()

    def read_csv_data(self):

        self.bd = pd.read_csv  ('F:\\COWS\\data\\csv_files\\birth_death.csv')

        self.heifbd = pd.read_csv  ('F:\\COWS\\data\\csv_files\\heifers.csv')
        self.lb = pd.read_csv  ('F:\\COWS\\data\\csv_files\\live_births.csv')
        self.i  = pd.read_csv  ('F:\\COWS\\data\\csv_files\\insem.csv')
        self.sd = pd.read_csv  ('F:\\COWS\\data\\csv_files\\stop_dates.csv')
        self.u  = pd.read_csv  ('F:\\COWS\\data\\csv_files\\ultra.csv')
        
        return [self.bd, self.lb,
                self.i, self.sd,
                self.u
                ]
       

    def write_to_csv(self):
    
        self.bd.to_csv       ('F:\\COWS\\data\\insem_data\\insem_backup\\birth_death\\birth_death_'+tdy+'.csv')
        self.heifbd.to_csv   ('F:\\COWS\\data\\insem_data\\insem_backup\\heifer_birth_death\\heifers_'+tdy+'.csv')        
        self.lb.to_csv       ('F:\\COWS\\data\\insem_data\\insem_backup\\live_births\\live_births_'+tdy+'.csv')
        self.i.to_csv        ('F:\\COWS\\data\\insem_data\\insem_backup\\insem\\insem_'            +tdy+'.csv')
        self.sd.to_csv       ('F:\\COWS\\data\\insem_data\\insem_backup\\stop_dates\\stop_dates_'  +tdy+'.csv')
        self.u.to_csv        ('F:\\COWS\\data\\insem_data\\insem_backup\\ultra\\ultra_'            +tdy+'.csv')
        
        self.bd.to_csv       ('E:\\COWS\\data_backup\\insem_data\\birth_death\\birth_death_'+tdy+'.csv')
        self.heifbd.to_csv   ('E:\\COWS\\data_backup\\insem_data\\heifer_birth_death\\heifers_'+tdy+'.csv')
        self.lb.to_csv       ('E:\\COWS\\data_backup\\insem_data\\live_births\\live_births_'+tdy+'.csv')
        self.i.to_csv        ('E:\\COWS\\data_backup\\insem_data\\insem\\insem_'            +tdy+'.csv')
        self.sd.to_csv       ('E:\\COWS\\data_backup\\insem_data\\stop_dates\\stop_dates_'  +tdy+'.csv')
        self.u.to_csv        ('E:\\COWS\\data_backup\\insem_data\\ultra\\ultra_'            +tdy+'.csv')


if __name__ == "__main__":
    obj = InsemDataBackup()
    obj.load_and_process()    