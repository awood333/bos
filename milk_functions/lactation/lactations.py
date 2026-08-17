'''Lactations.py'''
import inspect
import pandas as pd
from container import get_dependency


class Lactations:
    def __init__(self):
        print(f"Lactations instantiated by: {inspect.stack()[1].filename}")
        self.LB = None
        self.MB = None
        self.SD = None
        self.wy_id_list = None
        self.alive_ids = None
        self.L1 = self.L2 = self.L3 = self.L4 = self.L5 = self.L6 = None
        self.live_L1 = self.live_L2 = self.live_L3 = self.live_L4 = self.live_L5 = self.live_L6 = None
        self.headers = None
        self.alive_ids_int = None
        self.all_ids = None
        self.milking = None
        self.milking_weekly = None

    def load(self):

        self.LB     = get_dependency('lactation_basics')
        self.MB     = get_dependency('milk_basics')
        self.SD     = get_dependency('status_data')
        self.process()
        
    def process(self):

        self.all_ids        = self.LB.headers #wy_ids - str
        self.alive_ids  = self.SD.alive_ids_today

        #methods
        self.create_separate_lactations()
        self.create_live_lactations()
        self.create_live_lactations_weekly()
        self.create_milking()
        
    
    def create_separate_lactations(self):

        headers = self.all_ids
        
        L = self.LB.lactations_array # all wy_ids -- 1000 rows each
        print('L.shape', L.shape)
        
        #3rd dimension is the lactations dim
        self.L1 = pd.DataFrame(L[:,:,0], columns=headers)
        self.L2 = pd.DataFrame(L[:,:,1], columns=headers)
        self.L3 = pd.DataFrame(L[:,:,2], columns=headers)
        self.L4 = pd.DataFrame(L[:,:,3], columns=headers)
        self.L5 = pd.DataFrame(L[:,:,4], columns=headers)
        self.L6 = pd.DataFrame(L[:,:,5], columns=headers)
       
        return [self.L1,self.L2,self.L3,
                self.L4,self.L5,self.L6]
    

    def create_live_lactations(self):
        
        alive_ids = [str(x) for x in self.alive_ids]
        self.live_L1 = self.L1.loc[:,alive_ids]
        self.live_L2 = self.L2.loc[:,alive_ids]
        self.live_L3 = self.L3.loc[:,alive_ids]
        self.live_L4 = self.L4.loc[:,alive_ids]
        self.live_L5 = self.L5.loc[:,alive_ids]
        self.live_L6 = self.L6.loc[:,alive_ids]

        return [self.live_L1, self.live_L2 , self.live_L3,
                   self.live_L4, self.live_L5, self.live_L6]


    def create_live_lactations_weekly(self):
        """Convert daily live-lactation frames to weekly averages."""
        live_frames = [
            self.live_L1, self.live_L2, self.live_L3,
            self.live_L4, self.live_L5, self.live_L6,
        ]

        weekly = [
            df.where(df != 0)
              .groupby(df.index // 7)
              .mean()
              .fillna(0)
              .head(45)
            for df in live_frames
        ]

        (self.live_L1_weekly, self.live_L2_weekly, self.live_L3_weekly,
         self.live_L4_weekly, self.live_L5_weekly, self.live_L6_weekly) = weekly

        return weekly

    def create_milking(self):
        """Build a DataFrame of each cow's current ongoing lactation."""
        if self.LB.ongoing_lactations is None:
            self.LB.create_ongoing_lactations()

        ongoing = self.LB.ongoing_lactations
        lact_dfs = {
            1: self.L1, 2: self.L2, 3: self.L3,
            4: self.L4, 5: self.L5, 6: self.L6
        }

        milking = pd.DataFrame(0.0, index=self.L1.index, columns=self.L1.columns)

        for cow_str in self.L1.columns:
            cow_int = int(cow_str)
            lact_num = ongoing.get(cow_int)
            if pd.notna(lact_num):
                lact_df = lact_dfs[int(lact_num)]
                milking[cow_str] = lact_df[cow_str]

        self.milking = milking
        self.milking_weekly = (
            milking.where(milking != 0)
                   .groupby(milking.index // 7)
                   .mean()
                   .fillna(0)
                   .head(45)
        )

        return self.milking, self.milking_weekly


if __name__ == "__main__":
    obj=Lactations()
    obj.load()     