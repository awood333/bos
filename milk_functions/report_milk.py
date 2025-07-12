'''report_milk.py'''
import csv
from itertools import zip_longest
import pandas as pd

class ReportMilk:
    def __init__(self):
        
        
        self.report_milk  =     self.createReportMilk()
        self.write_to_csv()
            
    def createReportMilk(self):
        tenday = pd.read_csv("F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\tenday.csv")
        halfday = pd.read_csv("F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\halfday.csv")
        groups = pd.read_csv("F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\milking_groups.csv")

        report_milkxl = pd.concat([tenday, halfday, groups], axis=1)
        report_milkxl = report_milkxl.fillna('')  # Replace NaN with blank
        report_milkxl.to_excel("F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\report_milk.xlsx")



       # Build the combined header row
        header = (
            list(tenday.columns) +  #+ [""] +
            list(halfday.columns)+ #[""] +
            list(groups.columns) 
            
        )


        # Read CSV files into lists of rows using pandas DataFrames already loaded
        rows1 = tenday.astype(str).values.tolist()
        rows2 = halfday.astype(str).values.tolist()
        rows3 = groups.astype(str).values.tolist()
        
        # Concatenate rows side-by-side, padding with empty strings
        output_rows = [header]
        for r1, r2, r3 in zip_longest(rows1, rows2, rows3, fillvalue=[]):
            # Insert blank column between each
            row = []
            row.extend(r1)
            # row.append("")
            row.extend(r2)
            # row.append("")
            row.extend(r3)
            output_rows.append(row)

        # Write to output CSV
        with open('report_milk.csv', 'w', newline='', encoding='utf-8') as fout:
            writer = csv.writer(fout)
            writer.writerows(output_rows)
            
        report_milk1 = pd.DataFrame(output_rows[1:], columns=output_rows[0])
        report_milk1 = report_milk1.fillna('')            
        self.report_milk = report_milk1
        return self.report_milk
        
    def write_to_csv(self):
        self.report_milk.to_csv("F:\\COWS\\data\\milk_data\\totals\\milk_aggregates\\report_milk.csv")            
            
        
             
    
if __name__ == "__main__":
    ReportMilk()