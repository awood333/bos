import inspect
import pandas as pd
from milk_functions.milkaggregates import MilkAggregates
from milk_functions.milking_groups import MilkingGroups


class ReportMilk:
    def __init__(self, milk_aggregates=None, milking_groups=None):
        
        print(f"ReportMilk instantiated by: {inspect.stack()[1].filename}")
        self.MA = milk_aggregates or MilkAggregates()
        self.MG = milking_groups  or MilkingGroups(milk_aggregates=self.MA)
        
        self.tenday, self.halfday, self.groups = self.createReportMilk()

    def createReportMilk(self):
        

        tenday  = self.MA.tenday.copy()
        halfday = self.MA.halfday.copy()
        groups  = self.MG.milking_groups.copy()

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
        }

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
        groups_formatted = format_dataframe(groups, column_formats)

        return tenday_formatted, halfday_formatted, groups_formatted