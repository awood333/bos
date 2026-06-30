import inspect
import pandas as pd
from container import get_dependency


class ReportMilk:
    def __init__(self):
        
        print(f"ReportMilk instantiated by: {inspect.stack()[1].filename}")

        self.MA = None
        self.WG = None
        # self.CompareGroups = None
        self.tenday_formatted = None
        self.halfday_formatted = None
        self.WB_groups_formatted = None

    def load_and_process(self):

        self.MA = get_dependency('milk_aggregates')
        self.WG = get_dependency('whiteboard_groups')

# methods
        self.tenday_formatted, self.halfday_formatted, self.WB_groups_formatted = self.createReportMilk()
        
        from sql_db_related.neon_connect import get_engine
        engine = get_engine()
        self.write_to_neon(engine)


    def write_to_neon(self, engine):
        with engine.begin() as conn:
            self.tenday_formatted.to_sql(
                'tenday_formatted', conn,
                if_exists='replace', index=False
            )
            print(f"[neon] tenday_formatted written: {self.tenday_formatted.shape}")

            self.halfday_formatted.to_sql(
                'halfday_formatted', conn,
                if_exists='replace', index=False
            )
            print(f"[neon] halfday_formatted written: {self.halfday_formatted.shape}")

            self.WB_groups_formatted.to_sql(
                'wb_groups_formatted', conn,
                if_exists='replace', index=False
            )
            print(f"[neon] wb_groups_formatted written: {self.WB_groups_formatted.shape}")


    def createReportMilk(self): 

        tenday  = self.MA.tenday.copy()
        halfday = self.MA.halfday.copy()
        WB_groups  = self.WG.whiteboard_groups_tenday
        # CompareGroups = self.CompareGroups.compare_groups()


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
            'wy_id': '{:.0f}',
            # 'wy_id_1': '{:.0f}',
            # 'cow_id': '{:.0f}',
            # 'wy_id_2': '{:.0f}',
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
        

        #indentation is correct -- nested function
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

        # Format whiteboard groups (no pre-sort; WB_groups may not have 'avg')
        groups_formatted1 = format_dataframe(WB_groups, column_formats)

        # Slice tenday to get wy_id and the dynamic 10-day columns, excluding the last summary row
        cols = tenday_formatted.columns
        ten_day_cols = list(cols[11:18])
        tenday_part = tenday_formatted.loc[tenday_formatted.index[:-1], ['wy_id'] + ten_day_cols]

        # Merge on wy_id so that group info lines up with the correct 10-day values
        groups_merged = pd.merge(groups_formatted1, tenday_part, on='wy_id', how='left', sort=False)

        # Sort numerically by avg if present, then drop helper column
        if 'avg' in groups_merged.columns:
            groups_merged['avg_sort'] = pd.to_numeric(groups_merged['avg'], errors='coerce')
            groups_formatted = (
                groups_merged
                .sort_values('avg_sort', ascending=False)
                .drop(columns=['avg_sort'])
                .reset_index(drop=True)
            )
        else:
            groups_formatted = groups_merged.reset_index(drop=True)

        return tenday_formatted, halfday_formatted, groups_formatted
    
if __name__ == "__main__":
    obj = ReportMilk()
    obj.load_and_process()    