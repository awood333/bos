"""
Class to create a DataFrame for cows with no insemination check.
Fields: WY_id, i_date, age_insem
Logic: If 'age insem' is null and 'status' is 'M', select those rows.
Uses: get_dependency from container.py and allx from insem_ultra_data.py
"""

import pandas as pd
from container import get_dependency


class NoInsemCheck:
	def __init__(self):
		# Only set up dependency, do not process data yet
		insem_ultra_data = get_dependency('insem_ultra_data')
		self.allx = insem_ultra_data.allx.copy()
		self.no_insem_check = None
		self.create_get_no_insem_check = None

	def load_and_process(self):
		# Register the method for creating no insem check DataFrame
		self.create_get_no_insem_check = self._create_get_no_insem_check
		self.no_insem_check = self.create_get_no_insem_check()

	def _create_get_no_insem_check(self):
		# Filter rows where 'age insem' is null and 'status' is 'M'
		mask = (
			self.allx['age insem'].isnull()
			& (self.allx['status'] == 'M')
		)
		# Include 'days milking' field
		no_insem_check = self.allx.loc[mask, ['WY_id', 'i_date', 'age insem', 'days milking']].copy()
		# Convert i_date to string for display
		no_insem_check['i_date'] = pd.to_datetime(no_insem_check['i_date'], errors='coerce').dt.date.astype(str)
		# Sort by 'days milking' ascending
		no_insem_check = no_insem_check.sort_values(by='days milking', ascending=True).reset_index(drop=True)
		return no_insem_check

	def get_no_insem_check(self):
		return self.no_insem_check

if __name__ == "__main__":
	obj = NoInsemCheck()
	obj.load_and_process()
	print(obj.get_no_insem_check())
