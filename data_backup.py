import pandas as pd
import numpy as np
from datetime import datetime
#
tdy1=today=np.datetime64('today','D')
tdy=str(tdy1)

bd=pd.read_csv  ('F:\\COWS\\data\\csv_files\\birth_death.csv')
lb=pd.read_csv  ('F:\\COWS\\data\\csv_files\\live_births.csv')
i=pd.read_csv   ('F:\\COWS\\data\\csv_files\\insem.csv')
sd=pd.read_csv  ('F:\\COWS\\data\\csv_files\\stop_dates.csv')
m=pd.read_csv   ('F:\\COWS\\data\\csv_files\\miscarries.csv')
u=pd.read_csv   ('F:\\COWS\\data\\csv_files\\ultra.csv')

#
# amliters=   pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_liters.csv')
# amwy=       pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\AM_wy.csv')
# pmliters=   pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_liters.csv')
# pmwy=       pd.read_csv     ('F:\\COWS\\data\\milk_data\\raw\\csv\\PM_wy.csv')
#

#write to csv
# #
bd.to_csv       ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\CSV files\\birth death\\birth death'+tdy+'.csv')
lb.to_csv       ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\CSV files\\live births\\live births'+tdy+'.csv')
i.to_csv        ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\CSV files\\insem\\insem'            +tdy+'.csv')
sd.to_csv       ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\CSV files\\stop dates\\stop dates'  +tdy+'.csv')
m.to_csv        ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\CSV files\\miscarries\\miscarries' +tdy+'.csv')
u.to_csv        ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\CSV files\\ultra\\ultra'           +tdy+'.csv')



# amliters.to_csv ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\milk backup\\rawmilk\\AM_liters\\AM_liters_'   +tdy+'.csv')
# amwy.to_csv     ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\milk backup\\rawmilk\\AM_wy\\AM_wy_'       +tdy+'.csv')
# pmliters.to_csv ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\milk backup\\rawmilk\\PM_liters\\PM_liters_'   +tdy+'.csv')
# pmwy.to_csv     ('C:\\Users\\alanw\\OneDrive\\Cows\\data backup\\milk backup\\rawmilk\\PM_wy\\PM_wy_'       +tdy+'.csv')

