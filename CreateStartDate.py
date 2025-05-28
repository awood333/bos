'''CreateStartDate.py'''

from datetime import datetime, timedelta
import pandas as pd
from MilkBasics import  MilkBasics 


class DateRange:
    def __init__(self):
        
        self.data = MilkBasics().data
        self.date_format = "ISO8601"
        self.startdate  = self.start_date()
        self.enddate_monthly    = self.end_date_monthly()
        self.enddate_daily = self.end_date_daily()

        self.date_range_daily      = self.create_date_range_daily()
        self.date_range_monthly_data  = self.create_date_range_monthly()
        
    def start_date(self):
        self.startdate = pd.to_datetime("2024-01-01")
        return self.startdate
    
    def end_date_monthly(self):

        today = datetime.now()
        first_day_of_current_month = today.replace(day=1)
        last_completed_month = first_day_of_current_month - timedelta(days=1)
        # first_day_of_last_completed_month = last_completed_month.replace(day=1)
        self.enddate_monthly = pd.to_datetime(last_completed_month)
        
        return  self.enddate_monthly
    
    def end_date_daily(self):
        enddate = self.data['milk'].index[-1]
        self.enddate_daily = pd.to_datetime(enddate)
        return self.enddate_daily
        
        
    def create_date_range_daily(self):
        self.date_range_daily = pd.date_range(start=self.startdate, end=self.enddate_daily, freq='D')
        
        return self.date_range_daily
    
    def create_date_range_monthly(self):
 
        start           = self.startdate
        end             = self.enddate_monthly
        drm = pd.date_range(start=start, end=end, freq='ME')
        self.date_range_monthly1 = drm
        
        year = drm.year
        month = drm.month
        day = drm.days_in_month
        
        self.date_range_monthly = pd.MultiIndex.from_arrays([year, month], names=['year','month'])
        self.date_range_monthly2 =pd.MultiIndex.from_arrays([year, month, day], names=['year','month','days'])
        
        self.date_range_monthly_data = [
            self.date_range_monthly, 
            self.date_range_monthly1, 
            self.date_range_monthly2]
    
        return self.date_range_monthly_data
    

if __name__ == "__main__":
    DateRange()
    