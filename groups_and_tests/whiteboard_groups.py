'''groups_and_tests/whiteboard_groups.py'''

import inspect
import pandas as pd
from container import get_dependency
from groups_and_tests.create_WB_groups import BuildWBGroups


class WhiteboardGroups:
    def __init__(self):
        print(f"WhiteboardGroups instantiated by: {inspect.stack()[1].filename}")
        self.tenday_avg = None
        self.allx = pd.DataFrame()
        self.whiteboard_groups_tenday = pd.DataFrame()

    def load_and_process(self):
        IUD = get_dependency('insem_ultra_data')
        MA = get_dependency('milk_aggregates')

        self.allx = IUD.allx
        tenday_avg_1 = MA.tenday.loc[:, ['wy_id', 'avg']]
        self.tenday_avg = tenday_avg_1.set_index('wy_id')

        # Use BuildWBGroups instead of querying Neon directly.
        builder = BuildWBGroups()
        builder.load()

        self.whiteboard_groups_tenday = self._from_builder(builder)

    def _from_builder(self, builder: BuildWBGroups) -> pd.DataFrame:
        """Build a latest-group snapshot from BuildWBGroups' per-date frames."""
        frames = []
        for group in builder.GROUP_ORDER:
            df = getattr(builder, f"{group}_df")
            if df.empty:
                continue

            long = df.melt(var_name='snapshot_date', value_name='wy_id', ignore_index=False)
            long = long.dropna(subset=['wy_id'])
            long['group_name'] = group
            long['snapshot_date'] = pd.to_datetime(long['snapshot_date'])
            frames.append(long)

        if not frames:
            return pd.DataFrame()

        wbg = pd.concat(frames, ignore_index=True)

        # Keep each wy_id's most recent assignment.
        wbg = wbg.sort_values('snapshot_date').drop_duplicates('wy_id', keep='last')

        # Merge tenday average.
        wbg = wbg.merge(self.tenday_avg, how='left', left_on='wy_id', right_index=True)

        # Merge cow attributes.
        days = self.allx.loc[:, ['wy_id', 'days_milking', 'u_read', 'expected_bdate']]
        wbg = wbg.merge(days, how='left', on='wy_id')

        # Sort by average milk value, reset, and put snapshot_date last.
        wbg = wbg.sort_values('avg', ascending=False).reset_index(drop=True)
        wbg = wbg[['wy_id', 'group_name', 'avg', 'days_milking', 'u_read', 'expected_bdate', 'snapshot_date']]

        return wbg


if __name__ == "__main__":
    obj = WhiteboardGroups()
    obj.load_and_process()
    print(obj.whiteboard_groups_tenday)