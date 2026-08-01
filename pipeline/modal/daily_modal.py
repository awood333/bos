'''milk_functions/report_milk/daily_modal_data.py'''

import sys
from pathlib import Path

# Add project root (bos_backend/) to sys.path so container module is found
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
import inspect

from pipeline.neon.format_for_neon import FormatForNeon


class DailyModal:
    def __init__(self):

        print(f"DailyModal instantiated by: {inspect.stack()[1].filename}")

        self.tenday = None
        self.halfday = None
        self.fullday = None
        self.WB_groups_tenday = None
        self.groups = None
        self.allx = None


        self.tenday_formatted = None
        self.halfday_formatted = None
        self.fullday_formatted = None
        self.WB_groups_formatted = None


        # one FormatForNeon instance per table
        self.tenday_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
                "avg": "float",
                "pct chg from avg": "percent",
            },
            positional_rules=[(1, 11, "int")],  # dynamic 10-day columns
        )
        self.halfday_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
                "AM": "float",
                "PM": "float",
            },
            positional_rules=[(0, 1, "int")],  # the wy_id column, whatever it's named today
        )
        self.fullday_fmt = FormatForNeon()  # date-indexed, values left native float
        
        
        self.groups_fmt = FormatForNeon(
            schema={
                "wy_id" : "int",                
                "group_name": "text",
                "avg": "float",
                "days_milking": "int",
                "u_read": "text",
                "expected_bdate": "date",
                "snapshot_date": "date"
            },
        )
     

    def load_and_process(self):
        
        from container import get_dependency
        self.MA = get_dependency('milk_aggregates')
        self.MAB = get_dependency('milk_aggregates_basic')
        self.WG = get_dependency('whiteboard_groups')

        #methods
        (self.tenday_formatted, self.halfday_formatted,
         self.fullday_formatted, self.WB_groups_formatted) = self.createDailyData()

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


    def createDailyData(self):
        """
        Pulls raw dependency dataframes and does ONLY the merge/slice logic
        specific to this report. No dtype coercion here — FormatForNeon
        handles that per-table in write_to_neon, at write time.
        """
        self.tenday = self.MA.tenday.copy()
        self.halfday = self.MA.halfday.copy()
        self.fullday = self.MAB.fullday.copy()
        self.WB_groups_tenday = self.WG.whiteboard_groups_tenday.copy()

        return [self.tenday, self.halfday, self.fullday, self.WB_groups_tenday,
]


if __name__ == "__main__":
    obj = DailyModal()
    obj.load_and_process()