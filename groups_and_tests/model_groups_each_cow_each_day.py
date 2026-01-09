'''
groups_and_tests.model_groups_each_cow_each_day
'''

import json
import pandas as pd
from datetime import datetime
from container import get_dependency

class ModelGroupsEachCowEachDay:
	def __init__(self):
		self.DateRange   = get_dependency('date_range')
		self.MB          = get_dependency('milk_basics')
		self.json_path   = r"F:\\COWS\\data\\status\\model_groups_ids_dict.json"
		self.model_groups_dict = None
		self.df = None

		# startdate = None
		# lastday = None
		# WY_ids = None

	def load_and_process(self):
		startdate 	= pd.to_datetime(self.DateRange.startdate)
		lastday 	= pd.to_datetime(self.MB.lastday)
		WY_ids 		= self.MB.WY_ids
	
		# Load the JSON data
		with open(self.json_path, "r", encoding="utf-8") as f:
			self.model_groups_dict = json.load(f)

		# Prepare WY_id columns (1-311 as strings)
		# wy_ids = [str(i) for i in range(1, 312)]

		# Prepare date range from 2024-01-01 to the latest date in the JSON
		# all_dates = set()
		# for group in self.model_groups_dict.values():
		# 	all_dates.update(group.keys())
		# if all_dates:
		# 	max_date = max(all_dates)
		# else:
		# 	max_date = startdate

		date_range = pd.date_range(start=startdate, end=lastday, freq="D")

		# Create empty DataFrame
		result_df = pd.DataFrame(index=date_range.strftime("%Y-%m-%d"), columns=WY_ids)

        # Fill in group labels
		group_map = {
            "fresh_ids": "F",
            "group_A_ids": "A",
            "group_B_ids": "B",
            "group_C_ids": "C"
        }

		for group_key, label in group_map.items():
			group = self.model_groups_dict.get(group_key, {})
			for date, ids in group.items():
				date_str = date[:10]
				if date_str in result_df.index:
					for wy_id in ids:
						wy_id_int = str(int(float(wy_id)))
						if wy_id_int in result_df.columns:
							result_df.at[date_str, wy_id_int] = label
							
		self.df = result_df
		return self.df

	def save_to_csv(self):
		self.df.to_csv("F:\\COWS\\data\\status\\model_groups_each_cow_each_day.csv")


# Example usage
if __name__ == "__main__":
	mg = ModelGroupsEachCowEachDay()
	df = mg.load_and_process()
	mg.save_to_csv()
