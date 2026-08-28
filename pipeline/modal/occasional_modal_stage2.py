'''pipeline/modal/occasional_modal_stage2.py'''

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
import inspect
from container import get_dependency
from pipeline.neon.format_for_neon import FormatForNeon


class OccasionalModalStage2:
    """
    Stage 2: anything that reads back a table Stage 1 just wrote to
    Neon (here: ipiv_pivot_table reads ipiv_data_formatted). Must not
    run until Stage 1's write_to_neon() transaction has fully
    committed — see ModalOrchestrator.
    """
    def __init__(self):

        print(f"OccasionalModalStage2 instantiated by: {inspect.stack()[1].filename}")

        self.ipiv_pivot_table = None
        self.ipiv_pivot_table_formatted = None
        self.net_revenue_table = None
        self.net_revenue_table_formatted = None


        self.ipiv_pivot_table_fmt = FormatForNeon(
            schema={
                "wy_id": "int",
                "u_read": "text",
                "days_milking": "int",
            },
            positional_rules=[(3, None, "date")],  # every lact_num column, however many exist
        )

        self.net_revenue_table_fmt = FormatForNeon(
            schema={
                "datex": "date",
                "income": "float",
                "cost": "float",
                "net_revenue": "float",
            },
            # positional_rules=[(3, None, "date")],  # every lact_num column, however many exist
        )

    def load_and_process(self):

        self.IPIVT  = get_dependency('ipiv_pivot_table')
        self.NR     = get_dependency('net_revenue')
        self.FB     = get_dependency('finance_basics')

        [self.ipiv_pivot_table_formatted, 
         self.net_revenue_table_formatted] = self.createOccasionalData()
        


        from sql_db_related.neon_connect import get_engine
        engine = get_engine()
        self.write_to_neon(engine)

    def write_to_neon(self, engine):
        with engine.begin() as conn:

            self.ipiv_pivot_table_fmt.write_conn(
                self.ipiv_pivot_table_formatted, 'ipiv_pivot_table_formatted', conn, pk_col='wy_id')

            self.net_revenue_table_fmt.write_conn(
                self.net_revenue_table_formatted, 'net_revenue_table_formatted', conn, pk_col='datex')

#NOTE: the df's referenced here are only the 'less occasional' ones in addition to the ones in stage1


    def createOccasionalData(self):
        self.ipiv_pivot_table = self.IPIVT.ipiv_pivot_table.copy()
        
        self.net_revenue_table= self.NR.net_revenue_monthly.copy()
        # Convert PeriodIndex (monthly periods) to a real date column
        self.net_revenue_table.index = self.net_revenue_table.index.to_timestamp()
        self.net_revenue_table.index.name = 'datex'
        self.net_revenue_table = self.net_revenue_table.reset_index()
        
        return self.ipiv_pivot_table, self.net_revenue_table


if __name__ == "__main__":
    obj = OccasionalModalStage2()
    obj.load_and_process()