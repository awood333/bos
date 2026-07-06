'''milk_functions/report_milk/daily_modal_class.py

Modal-facing version of the milk reporting pipeline. Pulls tenday, halfday,
fullday, whiteboard groups, and next-ultra-check dependencies, applies only
the type-safety conversions Neon actually needs (nullable Int64 for id
columns, native datetime64 for dates, real None for missing text), and
writes typed tables to Neon. All display formatting (decimal places,
colors, alignment) is left to bos_dashboard.
'''


import inspect
import pandas as pd
from container import get_dependency
from insem_functions.next_ultra_check import NextUltraCheck


class DailyModal:
    def __init__(self):

        print(f"DailyModal instantiated by: {inspect.stack()[1].filename}")

        self.tenday_formatted = None
        self.halfday_formatted = None
        self.fullday_formatted = None
        self.WB_groups_formatted = None
        self.next_ultra_check_formatted = None

    def load_and_process(self):

        self.MA = get_dependency('milk_aggregates')
        self.MAB = get_dependency('milk_aggregates_basic')
        self.WG = get_dependency('whiteboard_groups')
        self.nuc = get_dependency('next_ultra_check')

        (self.tenday_formatted, self.halfday_formatted,
         self.fullday_formatted, self.WB_groups_formatted,
         self.next_ultra_check_formatted) = self.createDailyMilk()

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

            self.fullday_formatted.to_sql(
                'fullday_formatted', conn,
                if_exists='replace', index=False
            )
            print(f"[neon] fullday_formatted written: {self.fullday_formatted.shape}")

            self.next_ultra_check_formatted.to_sql(
                'next_ultra_check_formatted', conn,
                if_exists='replace', index=False
            )
            print(f"[neon] next_ultra_check_formatted written: {self.next_ultra_check_formatted.shape}")

    def createDailyMilk(self):

        tenday = self.MA.tenday.copy()
        halfday = self.MA.halfday.copy()
        fullday = self.MAB.fullday.copy()
        WB_groups = self.WG.whiteboard_groups_tenday
        next_ultra_check = self.nuc.next_ultra_check

        # column_types marks ONLY the columns that need explicit handling
        # to avoid Neon getting an ambiguous dtype:
        #   - 'int'  : id/count columns that may contain NaN, which would
        #              otherwise silently upcast to float64 (e.g. wy_id
        #              231 -> 231.0). Int64 is pandas' nullable integer
        #              dtype, so NaN survives as real SQL NULL and the
        #              column still writes as an integer type.
        #   - 'date' : left as native datetime64, NOT stringified — this
        #              maps to Postgres 'timestamp'/'date' directly and
        #              is what lets bos_dashboard do range queries and
        #              its own date formatting.
        #   - 'text' : forces dtype + replaces pandas' 'nan'/'None' string
        #              artifacts with real None (-> SQL NULL), rather
        #              than leaving literal 'nan' text in the column.
        # Anything NOT listed here is left completely alone if it's
        # already numeric — no forced rounding, no stringification.
        column_types = {
            'ultra': 'text',
            'group': 'text',
            'wy_id': 'int',
            'milking days': 'int',
            'days milking': 'int',
            'expected bdate': 'date',
            'whiteboard group': 'text',
            'model group': 'text',
            'comp': 'text',
        }

        def apply_column_types(dfx, types):
            df_typed = dfx.copy()
            for col in df_typed.columns:
                spec = types.get(col)

                if spec == 'date':
                    df_typed[col] = pd.to_datetime(df_typed[col], errors='coerce')

                elif spec == 'int':
                    df_typed[col] = pd.to_numeric(
                        df_typed[col], errors='coerce'
                    ).round(0).astype('Int64')

                elif spec == 'text':
                    df_typed[col] = df_typed[col].astype(str)
                    df_typed[col] = df_typed[col].replace(
                        ['nan', 'NaN', 'None', '<NA>'], None
                    )

                else:
                    # No spec: leave numeric columns as native float/int.
                    # Only touch object-dtype columns, and only to swap
                    # NaN-like values for real None — never a blanket
                    # str() cast that would silently text-ify a numeric
                    # column we forgot to list.
                    if not pd.api.types.is_numeric_dtype(df_typed[col]):
                        df_typed[col] = df_typed[col].where(
                            df_typed[col].notna(), None
                        )

            return df_typed

        def type_fullday(dfx):
            """fullday's date lives in the index (named 'datex' from
            MAB.fullday_calc). Keep it native datetime64 and reset into
            a 'date' column — do NOT stringify. Value columns (per-cow
            wy_id) are left as the float64 they already are; no rounding
            needed, Neon stores full precision natively as float8."""
            df_typed = dfx.copy()
            df_typed.index = pd.to_datetime(df_typed.index, errors='coerce')
            df_typed.index.name = 'date'
            df_typed = df_typed.reset_index()
            return df_typed

        tenday_formatted = apply_column_types(tenday, column_types)
        # Dynamic 10-day columns (index 1-10): nullable integer, matching
        # the same 'int' treatment as wy_id — no zero-padded strings.
        for idx in range(1, 11):
            col = tenday_formatted.columns[idx]
            tenday_formatted[col] = pd.to_numeric(
                tenday_formatted.iloc[:, idx], errors='coerce'
            ).round(0).astype('Int64')

        halfday_formatted = apply_column_types(halfday, column_types)

        fullday_formatted = type_fullday(fullday)

        groups_formatted1 = apply_column_types(WB_groups, column_types)

        next_ultra_check_formatted = apply_column_types(next_ultra_check, column_types)

        # Slice tenday to get wy_id and the dynamic 10-day columns, excluding the last summary row
        cols = tenday_formatted.columns
        ten_day_cols = list(cols[11:18])
        tenday_part = tenday_formatted.loc[tenday_formatted.index[:-1], ['wy_id'] + ten_day_cols]

        # Merge groups/tenday on wy_id — both sides are Int64 via the
        # same 'int' spec, so this is a clean same-dtype join.
        groups_merged = pd.merge(groups_formatted1, tenday_part, on='wy_id', how='left', sort=False)

        # 'avg' stays a native float if present — sort directly, no helper column.
        if 'avg' in groups_merged.columns:
            groups_formatted = (
                groups_merged
                .sort_values('avg', ascending=False)
                .reset_index(drop=True)
            )
        else:
            groups_formatted = groups_merged.reset_index(drop=True)

        return tenday_formatted, halfday_formatted, fullday_formatted, groups_formatted, next_ultra_check_formatted


if __name__ == "__main__":
    obj = DailyModal()
    obj.load_and_process()