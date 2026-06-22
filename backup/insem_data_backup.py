'''RunInsemDataBackup.py'''

import pandas as pd 

tdy = pd.Timestamp('now').strftime('%Y-%m-%d %H_%M_%S')

class InsemDataBackup:
    def __init__(self):
        
        self.bd, self.lb, self.i, self.sd, self.u  = self.read_csv_data()
        self.write_to_csv()



    def read_csv_data(self):
        self.bd     = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "basic_data" / "birth_death.csv")
        self.heifbd = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "basic_data" / "heifers.csv")
        self.lb     = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "basic_data" / "live_births.csv")
        self.i      = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "basic_data" / "insem.csv")
        self.sd     = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "basic_data" / "stop_dates.csv")
        self.u      = pd.read_csv(Path.home() / "gdrive_mount" / "COWS" / "basic_data" / "ultra.csv")
        return [self.bd, self.lb, self.i, self.sd, self.u]

    def write_to_csv(self):
        self.bd     .to_csv(Path.home() / "cows_data" / "basic_data" / "birth_death" /           f"birth_death_{tdy}.csv")
        self.heifbd .to_csv(Path.home() / "cows_data" / "basic_data" / "heifer_birth_death" /    f"heifers_{tdy}.csv")
        self.lb     .to_csv(Path.home() / "cows_data" / "basic_data" / "live_births" /           f"live_births_{tdy}.csv")
        self.i      .to_csv(Path.home() / "cows_data" / "basic_data" / "insem" /                 f"insem_{tdy}.csv")
        self.sd     .to_csv(Path.home() / "cows_data" / "basic_data" / "stop_dates" /            f"stop_dates_{tdy}.csv")
        self.u      .to_csv(Path.home() / "cows_data" / "basic_data" / "ultra" /                 f"ultra_{tdy}.csv")
        
        
if __name__ == "__main__":
    obj = InsemDataBackup()
