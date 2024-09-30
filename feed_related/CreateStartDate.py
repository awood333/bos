'''CreateStartDate.py'''

from datetime import datetime, timedelta
import pandas as pd

fullday = pd.read_csv('F:\\COWS\\data\\milk_data\\fullday\\fullday.csv')


class DateRange:
    def __init__(self):
        self.startdate  = self.StartDate()
        self.enddate_monthly    = self.EndDate_monthly()
        self.enddate_daily = self.EndDate_daily()

        self.date_range_daily      = self.create_date_range_daily()
        self.date_range_monthly    = self.create_date_range_monthly()
        
    def StartDate(self):
        self.startdate = pd.to_datetime("2024-1-1")
        return self.startdate
    
    def EndDate_monthly(self):

        today = datetime.now()
        first_day_of_current_month = today.replace(day=1)
        last_completed_month = first_day_of_current_month - timedelta(days=1)
        # first_day_of_last_completed_month = last_completed_month.replace(day=1)
        self.enddate_monthly = pd.to_datetime(last_completed_month)
        
        return  self.enddate_monthly
    
    def EndDate_daily(self):
        enddate = fullday['datex'].iloc[-1]
        self.enddate_daily = pd.to_datetime(enddate)
        return self.enddate_daily
        
        
    def create_date_range_daily(self):
        self.date_range_daily = pd.date_range(start=self.startdate, end=self.enddate_monthly, freq='D')
        
        return self.date_range_daily
    
    def create_date_range_monthly(self):
        start           = self.startdate
        end             = self.enddate_monthly
        self.date_range_monthly = pd.date_range(start=start, end=end, freq='ME')
        
        return self.date_range_monthly
    
    

if __name__ == "__main__":
    DateRange()
    