'''heifers.py'''

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from feed_functions.feedcost_basics import Feedcost_basics
from insem_functions.Insem_ultra_data import InsemUltraData

class Heifers:
    def __init__(self):

        self.FCB = Feedcost_basics()
        self.IUD = InsemUltraData()
        
        
        #feed costs
        self.dry_feed_cost  = self.FCB.current_feedcost['dry_cost'].loc['sum']
        self.dry_feed_kg    = self.FCB.current_feedcost['dry_kg'].loc['sum']
        self.dry_feed_cost_kg = self.dry_feed_cost / self.dry_feed_kg
        self.TMR_costper_kg = self.dry_feed_cost / self.dry_feed_kg
        self.bean_cost  =  self.FCB.current_feedcost['unit_price'].loc['beans']

        now = datetime.now()
        self.today = pd.to_datetime(now)
        self.rng = pd.date_range('2022-01-01', self.today)
        self.days_to_sale = 365+365+60  #days to 2months before calving...based on insem at 18m
        self.days_to_calf = 365+365+90 #820 this is cut-off for feed calc
        

        self.heifers, self.heifer_ids_list       = self.create_heifer_df()
        self.days3, self.date_index_col       = self.create_heifer_days()
        
        [self.milk_drinking_days, 
        self.cost_milk ]                = self.calc_milkdrinking_days()
        
        self.TMR_cost, self.TMR_current_cost   = self.calc_TMR_days()
        
        [self.yellow_beans_amt_days, 
        self.yellow_beans_cost]         = self.create_yellow_beans_days()
        
        self.align_days()
        
        
    
    def create_heifer_df(self):
        
        heifers1 = pd.read_csv("F:\\COWS\\data\\csv_files\\heifers_birth_death.csv", 
            header=0, index_col=None)
        heifers1['b_date'] = pd.to_datetime(heifers1['b_date'])
        heifers1['first_calf_bdate'] = pd.to_datetime(heifers1['first_calf_bdate'])
        heifers1['adj_bdate'] = pd.to_datetime(heifers1['adj_bdate'])
        heifers1['gone_date'] = pd.to_datetime(heifers1['gone_date'])
        
        heifers1['age_days'] = (self.today - heifers1['b_date']).dt.days

        heifers1.reset_index()
        self.heifer_ids_list = heifers1['heifer_ids'].to_list()
        
        self.heifers = heifers1
                 
        return self.heifers, self.heifer_ids_list
    
    def create_heifer_days(self):
    
        #   initialize dfs
        days2 = pd.DataFrame(index=self.rng)
        for i in self.heifers.index: #integer index from 0
            
            namex = self.heifer_ids_list[i]  # name will be the header of the days series
            birth_date = self.heifers.loc[i,'b_date'].date()
            calving_date = birth_date + timedelta(days=self.days_to_calf) #defined in constructor  integer 820 (days)

            start = birth_date
            stop = calving_date
            
            days_range = pd.date_range(start, stop)
            day_nums_series = pd.Series(range(0,len(days_range)), index=days_range, name=namex)
            days_nums_df = pd.DataFrame(day_nums_series, days_range)
            days1  = days_nums_df.reindex(self.rng)
                
            days2 = pd.concat([days2, days1], axis=1)
        # days3 = pd.concat([days3, days2], axis=0)
        # days3.columns = self.heifer_ids_list
            
        self.days = days2
        
        return self.days,  self.date_index_col
    
    
    def calc_milkdrinking_days(self):
        milk_drinking2=[]
        cost_milk2 = []
        str_cols = [str(x) for x in self.days.columns]
        
        for i in self.heifer_ids_list:
            
            days = self.days.loc[:,i]
                
            milk_drinking1a = days[ 
                (days > 0) & (days <= 90)
                ].dropna()
            
            milk_drinking1b = len(milk_drinking1a)
            cost_milk1       = milk_drinking1b * 22
            
            milk_drinking2.append(milk_drinking1b)
            cost_milk2.append(cost_milk1)

        
        self.milk_drinking_days = pd.DataFrame([milk_drinking2], columns=self.heifer_ids_list)
        self.cost_milk = pd.DataFrame([cost_milk2], columns=self.heifer_ids_list)    
        return self.milk_drinking_days, self.cost_milk
    
    def calc_TMR_days(self):
        
        TMR_cost2 = TMR_cost3 = pd.DataFrame()

        
        for i in self.heifer_ids_list:
            heif = self.heifers
            heif = heif.set_index('heifer_ids_list', drop=True)
            days = self.days.loc[:,i]     
            max_days = days.max()
            days_left = max_days - 90  # = days after 90 day milkdrinking 
            
            birth_date = pd.to_datetime(heif.loc[i,'birth_date'])
            TMR_kg = self.dry_feed_kg
            TMR_proportional_kg = TMR_kg / 90
            # TMR_cost = self.dry_feed_cost
            
#cow moves to dry adult cows when preg - preg date should be entered manually and = ultra_date             
            if not pd.isna(birth_date):  
                end_date1   = birth_date
                end_date   = pd.to_datetime(end_date1)
            
            elif pd.isna(birth_date):
                end_date = self.today
                
                

            if max_days<90:
                pass

            elif days_left >0 :
                
                #set start / stop dates
                    
                start_TMR_growth1 = days[days == 90].index  #date when calf is 90 days old             
                start_TMR_growth   = start_TMR_growth1.to_pydatetime()[0]   # converts from timestamp to datetime

                if days_left < 275:     #stop date is from 90 + however many days the calf is not preg (as of 'today')
                    stop_TMR_growth1 = days[days == (90 + days_left)].index  
                    stop_TMR_growth = stop_TMR_growth1.to_pydatetime()[0]
                
                elif days_left >= 275:  #calf is alive for the full 6months of TMR_growth rations
                    stop_TMR_growth1 = days[days == (90+270)].index  
                    stop_TMR_growth = stop_TMR_growth1.to_pydatetime()[0]
                    
                    
                    
                # start stop dates are set
                TMR_growth_range        = pd.date_range(start_TMR_growth, stop_TMR_growth)
                TMR_growth_amt_array    = np.cumsum(np.full(len(TMR_growth_range), TMR_proportional_kg))
                TMR_growth_cost_array   = TMR_growth_amt_array * self.TMR_costper_kg
                
                
            # this section adds 'regular' feed (at the max ~20kg/day) 
            if max_days < 365: 
                 TMR_regular_cost_array  = np.array([])
                 
                 
            elif max_days >= 365:     # TMR_growth + milkdrinking = 90+180=270
                
                start_TMR_regular1  = days[days == 366].index  
                start_TMR_regular   = start_TMR_regular1[0]
                  
                TMR_regular_range       = pd.date_range(start_TMR_regular, end_date)
                TMR_regular_cost_array  = np.full(len(TMR_regular_range), self.dry_feed_cost)   #cost/cow is constant in the 'regular' phase
              

            TMR_cost1       = np.concatenate([TMR_growth_cost_array, TMR_regular_cost_array], axis=0)

            #convert from numpy to df
            TMR_cost2     = pd.Series(TMR_cost1, name=i)
            
            #reindex to start at 90 so that the 90 days of milkdrinking will fit
            TMR_cost2.index       = pd.RangeIndex(start=90, stop= 90 + len(TMR_cost2) )
            
            TMR_cost3 = pd.concat([TMR_cost3, TMR_cost2], axis=1)   #creates df

            
            # reinitialize
            TMR_cost1 = TMR_cost2 =  np.array([])
            
        self.TMR_cost = TMR_cost3
        self.TMR_current_cost = self.TMR_cost.sum(axis=0)
        
            
        return self.TMR_cost, self.TMR_current_cost
    
    def create_yellow_beans_days(self):
        
        yellow_beans_amt_days2 = pd.DataFrame()
        yellow_beans_cost2 = pd.DataFrame()
        
        for i in self.heifer_ids_list:
            
            heif = self.heifers
            heif = heif.set_index('heifer_ids_list', drop=True)
            days = self.days.loc[:,i]     
            max_days = days.max()
            amt_beans = 1   #manually enter current amount (kgs)
            
            
            
            if max_days<120:
                if not  yellow_beans_amt_days2.empty:
                    blank_col = pd.Series( name=i)
                    yellow_beans_amt_days2  = pd.concat([yellow_beans_amt_days2, blank_col], axis=1)
                    yellow_beans_cost2      = pd.concat([yellow_beans_cost2, blank_col], axis=1)
                    
                if yellow_beans_amt_days2.empty:
                    yellow_beans_amt_days2  = pd.Series( name=i)
                    yellow_beans_cost2 = pd.Series( name=i)
            
            elif max_days>=120:
                start1 = days[days == 120].index
                if max_days <150:
                    stop1  = days[days == max_days].index
                    
                elif max_days >=150:
                    stop1 = days[days == 150].index
                
                start = start1[0]       #120 days after birth
                stop = stop1[0]
                
                yellow_beans_date_range     = pd.date_range(start, stop )
                yellow_beans_amt_days1      = np.full(len(yellow_beans_date_range), amt_beans)
                yellow_beans_amt_days_series= pd.Series(yellow_beans_amt_days1, name = i)
                
                yellow_beans_cost1      = yellow_beans_amt_days_series * self.bean_cost
                
                yellow_beans_amt_days2  = pd.concat([yellow_beans_amt_days2, yellow_beans_amt_days_series], axis=1)
                yellow_beans_cost2      = pd.concat([yellow_beans_cost2, yellow_beans_cost1], axis=1)
                
                yellow_beans_amt_days1 = yellow_beans_cost1  = np.array([])
                yellow_beans_cost1 = pd.Series()
                
        
        yellow_beans_amt_days2.index    = pd.RangeIndex(120, 120 + len(yellow_beans_amt_days2))
        yellow_beans_cost2.index        = pd.RangeIndex(120, 120 + len(yellow_beans_cost2))
            
        self.yellow_beans_amt_days  = yellow_beans_amt_days2
        self.yellow_beans_cost      = yellow_beans_cost2
            
        return self.yellow_beans_amt_days, self.yellow_beans_cost
     
    def align_days(self):
        
        new_index = pd.DataFrame()
        
        wy = self.heifer_ids_list
        heif1 = self.heifers
        heif1 = heif1.set_index('heifer_ids_list', drop=True)
        age = heif1['age_days']
        
        for i in wy:
            age1 = age[i]
            age_range = pd.RangeIndex(1,age1)
            
            milk_days1 = self.milk_drinking_days.loc[0,i]
            milk_index = pd.RangeIndex(1, milk_days1, name=i)
            milk_amt = 6
            milk_days1 = pd.Series(milk_amt, index=milk_index)
            milk_days = milk_days1.reindex(age_range, fill_value=0)

            bean_days1 = self.yellow_beans_amt_days[i]
            bean_index = pd.RangeIndex(120, (120+len(bean_days1)), step=1, name=i)
            bean_days1.index = bean_index
            bean_days = bean_days1.reindex(age_range, fill_value=0)

            TMR_days1 = self.TMR_amt_days[i]
            TMR_index = pd.RangeIndex(90, age1, step=1, name=i)
            # TMR_days1.index = TMR_index
            TMR_days = TMR_days1.reindex(age_range, fill_value=0)
            
            new_index = pd.DataFrame( index = age_range)
            days_df = pd.concat([new_index, milk_days, bean_days, TMR_days])
            
        
        return
            
     
if __name__ == "__main__":
    Heifers()