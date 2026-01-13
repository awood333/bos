"""
Class to create a DataFrame for next ultra check dates.
Fields: WY_id, i_date, age_insem, next_ultra_check_date
Logic: If 'age insem' is not null and 'u_date' is null, next_ultra_check_date = i_date + 40 days
Uses: get_dependency from container.py and allx from insem_ultra_data.py
"""

import pandas as pd
from container import get_dependency


class NextUltraCheck:
    def __init__(self):
        # Only set up dependency, do not process data yet
        insem_ultra_data = get_dependency('insem_ultra_data')
        self.allx = insem_ultra_data.allx.copy()
        self.next_ultra_check = None
        self.create_get_next_ultra_check = None

    def load_and_process(self):
        # Register the method for creating next ultra check DataFrame
        self.create_get_next_ultra_check = self._create_get_next_ultra_check
        self.next_ultra_check = self.create_get_next_ultra_check()

    def _create_get_next_ultra_check(self):
        # Filter rows where 'age insem' is not null and 'u_date' is null
        mask = (
            self.allx['age insem'].notnull()
            & self.allx['u_date'].isnull()
            & (self.allx['status'] == 'M')
        )
        next_ultra_check = self.allx.loc[mask, ['WY_id', 'i_date', 'age insem']].copy()
        # Calculate estimated ultra check date: i_date + 40 days
        next_ultra_check['i_date'] = pd.to_datetime(next_ultra_check['i_date'], errors='coerce').dt.date
        next_ultra_check['next ultra check date'] = (
            pd.to_datetime(next_ultra_check['i_date'], errors='coerce') + pd.to_timedelta(40, unit='D')
        ).dt.date
        # Ensure column is datetime64 for sorting
        next_ultra_check['next ultra check date'] = pd.to_datetime(next_ultra_check['next ultra check date'], errors='coerce')
        next_ultra_check = next_ultra_check.loc[next_ultra_check['next ultra check date'].sort_values(ascending=True).index].reset_index(drop=True)
        # Convert back to string date format for display
        next_ultra_check['i_date'] = next_ultra_check['i_date'].astype(str)
        next_ultra_check['next ultra check date'] = next_ultra_check['next ultra check date'].dt.date.astype(str)
        return next_ultra_check

    def get_next_ultra_check(self):
        return self.next_ultra_check

if __name__ == "__main__":
    obj = NextUltraCheck()
    obj.load_and_process()
    print(obj.get_next_ultra_check())