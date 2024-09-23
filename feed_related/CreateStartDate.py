'''CreateStartDate.py'''

from datetime import datetime
import pandas as pd

class DateRange:
    def __init__(self):
        self.startdate  = self.StartDate()
        self.enddate    = self.EndDate()
        self.date_range        = self.create_date_range()
        
    def StartDate(self):
        self.startdate = "1/1/2024"
        return self.startdate
    
    def EndDate(self):
        startdate   = self.StartDate
        thismonth   = datetime.now().month
        thisyear    = datetime.now().year
        startday    = 1
        self.enddate     = datetime(thisyear,thismonth, startday)

        return  self.enddate
        
        
    def create_date_range(self):
        start = pd.to_datetime(self.startdate)
        end = pd.to_datetime(self.enddate)
        return pd.date_range(start=start, end=end)
        
if __name__ == "__main__":
    dr = DateRange()
    dr.StartDate()
    dr.EndDate()
    dr.DateRange()
