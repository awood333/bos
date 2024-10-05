# formatting.py
import pandas as pd

class DataFrameFormatter:
    def __init__(self, df):
        self.df = df

    def infer_and_convert_columns(self):
        for col in self.df.columns:
            inferred_type = pd.api.types.infer_dtype(self.df[col], skipna=True)
 
            if inferred_type in ['string', 'mixed']:
                # Check if the column contains date separators
                if self.df[col].str.contains(r'[/\-_]').any():
  
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    except (ValueError, TypeError):
                        pass
                else:
                    pass

            elif inferred_type == 'integer' or inferred_type == 'float':
                # Convert numeric columns that are likely timestamps
                if self.df[col].apply(lambda x: len(str(x)) >= 10).all():  # Check if all values are likely timestamps
                    self.df[col] = pd.to_datetime(self.df[col], unit='ms', errors='coerce')
                else:
                    pass

                    continue

    def format_date_columns(self):
        for col in self.df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            self.df[col] = self.df[col].dt.strftime('%Y-%m-%d')

    def format_dataframe(self):
        self.infer_and_convert_columns()
        self.format_date_columns()
        return self.df

# Example usage:
# df_formatter = DataFrameFormatter(df)
# formatted_df = df_formatter.format_dataframe()