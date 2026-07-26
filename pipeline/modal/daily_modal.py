'''milk_functions/report_milk/daily_modal_data.py'''

import inspect
import pandas as pd
from container import get_dependency
from pipeline.neon.format_for_neon import FormatForNeon


class DailyModal:
    def __init__(self):

        print(f"DailyModal instantiated by: {inspect.stack()[1].filename}")

        self.tenday      = None
        self.halfday      = None
        self.fullday      = None
        self.WB_groups  = None
        self.groups      = None
        self.next_ultra_check = None
        self.i_u_merge   = None
        self.allx      = None



        self.tenday_formatted       = None
        self.halfday_formatted      = None
        self.fullday_formatted      = None
        self.WB_groups_formatted    = None
        self.next_ultra_check_formatted = None
        self.i_u_merge_formatted    = None
        self.allx_formatted         = None

        # one FormatForNeon instance per table
        self.tenday_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
                "milking days": "int",
                "days milking": "int",
                "expected bdate": "date",
            },
            positional_rules=[(1, 11, "int")],  # dynamic 10-day columns
        )
        self.halfday_fmt = FormatForNeon(
            schema={
                "AM": "float",
                "PM": "float",
            },
            positional_rules=[(0, 1, "int")],  # the wy_id column, whatever it's named today
        )
        self.fullday_fmt = FormatForNeon()  # date-indexed, values left native float
        self.groups_fmt = FormatForNeon(
            schema={
                "ultra": "text",
                "group": "text",
                "wy_id": "int",
                "whiteboard group": "text",
                "model group": "text",
                "comp": "text",
            },
        )
        self.nuc_fmt = FormatForNeon(
            schema={
                "ultra": "text",
                "group": "text",
                "wy_id": "int",
                "expected bdate": "date",
            },
        )
        self.ium_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
            },
        )
        
        self.allx_fmt = FormatForNeon(
            schema={
                "stop_calf_num": "int",
                "last_calf_bdate": "date",
                "last_calf_num": "int",
                "days_milking": "int",
                "i_calf_num": "int",
                "i_date": "date",
                "age_insem": "int",
                "u_calf_num": "int",
                "u_date": "date",
                "u_read": "text",
                "age_ultra": "int",
                "expected_bdate": "date",
                "exp_drydate": "date",
                "i_check": "int",
                "u_check1": "int",
                "u_check2": "int",
            }
        )
        
        
        
    def load_and_process(self):

        self.MA     = get_dependency('milk_aggregates')
        self.MAB    = get_dependency('milk_aggregates_basic')
        self.WG     = get_dependency('whiteboard_groups')
        self.NUC    = get_dependency('next_ultra_check')
        self.IUM    = get_dependency('i_u_merge')
        self.IUD    = get_dependency('insem_ultra_data')
        

        (self.tenday_formatted, self.halfday_formatted,
         self.fullday_formatted, self.WB_groups_formatted,
         self.next_ultra_check_formatted, self.i_u_merge_formatted,
         self.allx_formatted)     = self.createDailyData()

        from sql_db_related.neon_connect import get_engine
        engine = get_engine()
        self.write_to_neon(engine)

    def write_to_neon(self, engine):
        with engine.begin() as conn:

            self.tenday_fmt.write_conn(
                self.tenday_formatted, 'tenday_formatted', conn, pk_col='wy_id')

            self.halfday_fmt.write_conn(
                self.halfday_formatted, 'halfday_formatted', conn)

            self.fullday_fmt.write_conn(
                self.fullday_formatted, 'fullday_formatted', conn, indexed_date=True)

            self.groups_fmt.write_conn(
                self.WB_groups_formatted, 'wb_groups_formatted', conn, pk_col='wy_id')

            self.nuc_fmt.write_conn(
                self.next_ultra_check_formatted, 'next_ultra_check_formatted', conn, pk_col='wy_id')

            self.ium_fmt.write_conn(
                self.i_u_merge_formatted, 'iu_merge_formatted', conn)
            
            self.allx_fmt.write_conn(
                self.allx_formatted, 'allx_formatted', conn, pk_col='wy_id')
            

    def createDailyData(self):
        """
        Pulls raw dependency dataframes and does ONLY the merge/slice logic
        specific to this report. No dtype coercion here — FormatForNeon
        handles that per-table in write_to_neon, at write time.
        """
        self.tenday      = self.MA.tenday.copy()
        self.halfday     = self.MA.halfday.copy()
        self.fullday     = self.MAB.fullday.copy()
        self.WB_groups   = self.WG.whiteboard_groups_tenday.copy()
        self.next_ultra_check = self.NUC.next_ultra_check.copy()
        self.i_u_merge   = self.IUM.iu.copy()
        self.allx        = self.IUD.allx.copy()
        
        self.allx['updated'] = pd.Timestamp.now()
        
        # Slice tenday to get wy_id and the dynamic day columns, excluding
        # the last summary row. Preserved exactly as in the original —
        # cols[11:18], NOT the same range as the (1,11) dtype rule above.
        cols = self.tenday.columns
        ten_day_cols = list(cols[11:18]) #all the cols after the 10 days cols
        tenday_part = self.tenday.loc[self.tenday.index[:-1], ['wy_id'] + ten_day_cols] #these are to be used in the wbgroups panel

        tenday = pd.merge(self.WB_groups, tenday_part, on='wy_id', how='left', sort=False)

        if 'avg' in tenday.columns:
            self.groups = tenday.sort_values('avg', ascending=False).reset_index(drop=True)
        else:
            self.groups = tenday.reset_index(drop=True)

        return [self.tenday, self.halfday, 
                self.fullday, self.groups, 
                self.next_ultra_check, self.i_u_merge, 
                self.allx]

if __name__ == "__main__":
    obj = DailyModal()
    obj.load_and_process()