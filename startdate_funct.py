import pandas as pd

class CreateStartdate:
    def __init__(self):
        self.startdate, self.date_format = self.create_startdate()
        
        
    def create_startdate(self):
        startdate1   = '2023-6-1'
        date_format = '%Y-%m-%d %H:%M:%S'
        startdate = pd.to_datetime(startdate1)
        
        return startdate, date_format