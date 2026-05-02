'''RunInsemDataBackup.py'''

import pandas as pd 

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class InsemDataBackup:
    def __init__(self):
        
        self.bd, self.lb, self.i, self.sd, self.u  = self.read_csv_data()
        self.write_to_csv()



    def read_csv_data(self):
        from config_path import BASIC_DATA_DIR
        self.bd     = pd.read_csv(BASIC_DATA_DIR / "birth_death.csv")
        self.heifbd = pd.read_csv(BASIC_DATA_DIR / "heifers.csv")
        self.lb     = pd.read_csv(BASIC_DATA_DIR / "live_births.csv")
        self.i      = pd.read_csv(BASIC_DATA_DIR / "insem.csv")
        self.sd     = pd.read_csv(BASIC_DATA_DIR / "stop_dates.csv")
        self.u      = pd.read_csv(BASIC_DATA_DIR / "ultra.csv")
        return [self.bd, self.lb, self.i, self.sd, self.u]

    def write_to_csv(self):
        from config_path import (
            BIRTH_DEATH_DIR, HEIFER_BIRTH_DEATH_DIR, INSEM_DIR,
            LIVE_BIRTHS_DIR, STOP_DATES_DIR, ULTRA_DIR
        )
        self.bd     .to_csv(BIRTH_DEATH_DIR /           f"birth_death_{tdy}.csv")
        self.heifbd .to_csv(HEIFER_BIRTH_DEATH_DIR /    f"heifers_{tdy}.csv")
        self.lb     .to_csv(LIVE_BIRTHS_DIR /           f"live_births_{tdy}.csv")
        self.i      .to_csv(INSEM_DIR /                 f"insem_{tdy}.csv")
        self.sd     .to_csv(STOP_DATES_DIR /            f"stop_dates_{tdy}.csv")
        self.u      .to_csv(ULTRA_DIR /                 f"ultra_{tdy}.csv")
        
        
if __name__ == "__main__":
    obj = InsemDataBackup()
