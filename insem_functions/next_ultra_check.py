"""
Class to create a DataFrame for next ultra check dates.
Fields: WY_id, i_date, age_insem, estimated_ultra_check_date
Logic: If 'age insem' is not null and 'u_date' is null, estimated_ultra_check_date = i_date + 40 days
Uses: get_dependency from container.py and allx from insem_ultra_data.py
"""

import pandas as pd
from container import get_dependency

class NextUltraCheck:
    def __init__(self):
        # Get the insem_ultra_data dependency and access allx
        insem_ultra_data = get_dependency('insem_ultra_data')
        self.allx = insem_ultra_data.allx.copy()
        self.next_ultra_check = self._create_next_ultra_check_df()

    def _create_next_ultra_check_df(self):
        # Filter rows where 'age insem' is not null and 'u_date' is null
        mask = self.allx['age insem'].notnull() & self.allx['u_date'].isnull()
        next_ultra_check = self.allx.loc[mask, ['WY_id', 'i_date', 'age_insem']].copy()
        # Calculate estimated ultra check date: i_date + 40 days
        next_ultra_check['estimated_ultra_check_date'] = next_ultra_check['i_date'] + pd.to_timedelta(40, unit='D')
        return next_ultra_check

    def get_next_ultra_check(self):
        return self.next_ultra_check

if __name__ == "__main__":
    nuc = NextUltraCheck()
    print(nuc.get_next_ultra_check())