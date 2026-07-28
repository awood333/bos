'''pipeline/modal/occasional_modal_stage1.py'''

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
import inspect
import pandas as pd
from container import get_dependency
from pipeline.neon.format_for_neon import FormatForNeon


class OccasionalModalStage1:
    """
    Stage 1: builds and writes everything that depends ONLY on raw
    source dependencies — nothing here reads back another table this
    pipeline itself wrote to Neon. Safe to run in any order relative
    to other Stage 1 modules (e.g. DailyModal).
    """
    def __init__(self):

        print(f"OccasionalModalStage1 instantiated by: {inspect.stack()[1].filename}")

        self.next_ultra_check = None
        self.i_u_merge = None
        self.allx = None
        self.ipiv_data = None

        self.next_ultra_check_formatted = None
        self.i_u_merge_formatted = None
        self.allx_formatted = None
        self.ipiv_data_formatted = None

        self.nuc_fmt = FormatForNeon(
            schema={
                "ultra": "text",
                "group": "text",
                "wy_id": "int",
                "expected_bdate": "date",
            },
        )
        self.ium_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
            },
        )
        self.allx_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
                "status": "text",
                "last_stop_date": "date",
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
                "updated": "date",
            }
        )
        self.ipiv_data_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
                "lact_num": "int",
                "try_num": "int",
                "insem_date": "date",
            }
        )

    def load_and_process(self):

        self.NUC = get_dependency('next_ultra_check')
        self.IUM = get_dependency('i_u_merge')
        self.IUD = get_dependency('insem_ultra_data')
        self.IPIV = get_dependency('ipiv_data')

        (self.next_ultra_check_formatted, self.i_u_merge_formatted,
         self.allx_formatted, self.ipiv_data_formatted) = self.createOccasionalData()

        from sql_db_related.neon_connect import get_engine
        engine = get_engine()
        self.write_to_neon(engine)

    def write_to_neon(self, engine):
        with engine.begin() as conn:

            self.nuc_fmt.write_conn(
                self.next_ultra_check_formatted, 'next_ultra_check_formatted', conn, pk_col='wy_id')

            self.ium_fmt.write_conn(
                self.i_u_merge_formatted, 'iu_merge_formatted', conn)

            self.allx_fmt.write_conn(
                self.allx_formatted, 'allx_formatted', conn, pk_col='wy_id')

            self.ipiv_data_fmt.write_conn(
                self.ipiv_data_formatted, 'ipiv_data_formatted', conn,
                pk_col=['wy_id', 'lact_num', 'try_num'])

    def createOccasionalData(self):
        """
        Pulls raw dependency dataframes only. No dtype coercion here —
        FormatForNeon handles that per-table in write_to_neon, at write time.
        """
        self.next_ultra_check = self.NUC.next_ultra_check.copy()
        self.i_u_merge = self.IUM.iu.copy()
        self.allx = self.IUD.allx.copy()
        self.allx['updated'] = pd.Timestamp.now()
        self.ipiv_data = self.IPIV.ipiv_data.copy()

        return [self.next_ultra_check, self.i_u_merge,
                self.allx, self.ipiv_data]


if __name__ == "__main__":
    obj = OccasionalModalStage1()
    obj.load_and_process()