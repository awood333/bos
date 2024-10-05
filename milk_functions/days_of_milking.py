'''milk_functions\\days_of_milking.py'''

import pandas as pd
# from datetime import datetime, timedelta
from milk_functions.lactations import Lactations
from milk_functions.WetDryBasics import WetDryBasics
from feed_related.CreateStartDate import DateRange

    
class DaysOfMilking:
    def __init__(self):
        self.L = Lactations()
        self.WDB = WetDryBasics()
        self.DOM = self.L.day_of_milking4
        
        date_range              = DateRange()
        self.startdate          = date_range.startdate
        self.enddate_monthly    = date_range.enddate_monthly
        self.enddate_daily      = date_range.enddate_daily
        
        self.df_results             = self.processDaysOfMilking()
        self.milk_income_dash_vars  = self.get_dash_vars()
        self.write_to_csv()

        self.date_range_daily   = date_range.create_date_range_daily()
        self.date_range_monthly = date_range.create_date_range_monthly()

    def processDaysOfMilking(self):
        date_range = pd.date_range(start='2016-09-01', end=self.enddate_daily, freq='D')
        df = pd.DataFrame(index=date_range)
        series_list = []

        for i, sublist in enumerate(self.DOM[0]):
            concatenated_series = pd.Series()
            for j, series in enumerate(sublist):
                concatenated_series = pd.concat([concatenated_series, series])
            
            concatenated_series = concatenated_series.sort_index()
            concatenated_series = concatenated_series.reindex(date_range, fill_value=0)
            concatenated_series.name = f'list_{i}'
            series_list.append(concatenated_series)

        df = pd.concat([df] + series_list, axis=1)
        
        # Group series by their first numeral
        grouped_series = {}
        for col in df.columns:
            if col.startswith('list_'):
                group = col.split('_')[1]
                if group not in grouped_series:
                    grouped_series[group] = []
                grouped_series[group].append(df[col])

        # Concatenate grouped series and add to a new DataFrame
        concatenated_series_dict = {}
        for group, series_list in grouped_series.items():
            if len(series_list) == 1:  # XXXXX- Check if there's only one series
                concatenated_series = series_list[0]
            else:
                concatenated_series = pd.concat(series_list, axis=0)
            concatenated_series = concatenated_series.sort_index()
            concatenated_series = concatenated_series.reindex(date_range, fill_value=0)
            concatenated_series_dict[f'list_{group}'] = concatenated_series
            
        self.result_df = pd.DataFrame(concatenated_series_dict, index=df.index)


        return self.result_df


    
    def get_dash_vars(self):
        self.milk_income_dash_vars = self.result_df
        # {name: value for name, value in vars(self).items()
        #        if isinstance(value, (pd.DataFrame, pd.Series))}
        return self.milk_income_dash_vars  
    
    def write_to_csv(self):
        self.result_df.to_csv('F:\\COWS\\data\\milk_data\\lactations\\daysOfMilking.csv')



if __name__ == "__main__":
    days = DaysOfMilking()
