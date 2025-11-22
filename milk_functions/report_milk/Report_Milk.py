import inspect
import pandas as pd
from container import get_dependency


class ReportMilk:
    def __init__(self):
        
        print(f"ReportMilk instantiated by: {inspect.stack()[1].filename}")

        self.MA = None
        self.WG = None
        self.MG = None
        self.CompareGroups = None
        self.tenday = None
        self.halfday = None
        self.WB_groups = None

    def load_and_process(self):

        self.MA = get_dependency('milk_aggregates')
        self.WG = get_dependency('whiteboard_groups')
        self.MG = get_dependency('model_groups')
        self.CompareGroups  = get_dependency('compare_model_whiteboard_groups')

        self.tenday, self.halfday, self.WB_groups, self.CompareGroups = self.createReportMilk()


    def createReportMilk(self): 

        tenday  = self.MA.tenday.copy()
        halfday = self.MA.halfday.copy()
        WB_groups  = self.WG.whiteboard_groups_tenday.copy()
        CompareGroups = self.CompareGroups.compare_groups()


        column_formats = {
            'ultra': 'text',
            'group': 'text',
            'avg': '{:.1f}',
            'pct chg from avg': '{:.2f}',
            # 'chg from avg': '{:.2f}',
            'AM': '{:.1f}',
            'PM': '{:.1f}',
            # 'litres': '{:.1f}',
            # 'avg': '{:.1f}',
            'WY_id': '{:.0f}',
            # 'WY_id_1': '{:.0f}',
            # 'cow_id': '{:.0f}',
            # 'WY_id_2': '{:.0f}',
            'milking days': '{:.0f}',
            'days milking': '{:.0f}',
            'expected bdate': 'iso8601',
            # 'bdate (exp)': 'iso8601',
            'whiteboard group': 'text',
            'model group' : 'text',
            'comp' : 'text'
        }

# The method format_dataframe is defined to take two arguments: dfx (a DataFrame) and formats (a dictionary of column formatting rules).
# When you call tenday_formatted = format_dataframe(tenday, column_formats), you are passing:
# tenday as dfx
# column_formats as formats
# Inside the method, dfx refers to the DataFrame you passed (tenday in this case), 
# and formats refers to the formatting dictionary.
# So, each time you call format_dataframe with a different DataFrame (like tenday, halfday, or WB_groups), 
# it applies the formatting rules from column_formats to that DataFrame and returns a formatted copy.
        
        def format_dataframe(dfx, formats):
            df_formatted = dfx.copy()
            for col in df_formatted.columns:
                if col in formats:
                    if formats[col] == 'iso8601':
                        df_formatted[col] = pd.to_datetime(df_formatted[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    elif formats[col] != 'text':
                        df_formatted[col] = pd.to_numeric(df_formatted[col], errors='coerce')
                        df_formatted[col] = df_formatted[col].apply(lambda x, fmt=formats[col]: fmt.format(x) if pd.notna(x) else '') 
                    else:
                        df_formatted[col] = df_formatted[col].astype(str)
                        if col in ['ultra', 'u_read']:
                            df_formatted[col] = df_formatted[col].replace(['nan', 'NaN', 'None'], '')
                else:
                    df_formatted[col] = df_formatted[col].astype(str)
                    if col in ['ultra', 'u_read']:
                        df_formatted[col] = df_formatted[col].replace(['nan', 'NaN', 'None'], '')
                        
            return df_formatted

        tenday_formatted = format_dataframe(tenday, column_formats)
        for idx in range(1, 11):
            col = tenday_formatted.columns[idx]
            tenday_formatted[col] = pd.to_numeric(tenday_formatted.iloc[:, idx], errors='coerce').round(0).astype('Int64').astype(str)

        halfday_formatted = format_dataframe(halfday, column_formats)
        groups_formatted = format_dataframe(WB_groups, column_formats)
        CompareGroups_formatted = format_dataframe(CompareGroups, column_formats)

        return tenday_formatted, halfday_formatted, groups_formatted, CompareGroups_formatted
    
if __name__ == "__main__":
    obj = ReportMilk()
    obj.load_and_process()    