'''groups_and_tests/create_WB_groups.py'''
import inspect
import pandas as pd
from sql_db_related.neon_connect import get_engine
from container import get_dependency


class BuildWBGroups:

    def __init__(self):
        print(f"BuildWBGroups instantiated by: {inspect.stack()[1].filename}")
        self.engine = get_engine()
        self.GROUP_ORDER = ["fresh", "group_A", "group_B", "group_C", "sick"]
        self.start = None
        self.am_wy = None
        self.counts = None
        self.group_frames = {}

    def load(self):
        self.process()
        self.split_group_frames()

    def process(self):
        start = pd.to_datetime("2026-08-27")

        am_wy, counts = self._AM_wy_and_group_count_query()

        # Normalize dates
        am_wy["date"]   = pd.to_datetime(am_wy["date"]).dt.normalize()
        counts["datex"] = pd.to_datetime(counts["datex"]).dt.normalize()

        am_wy = am_wy[am_wy["date"] >= start]
        counts = counts[counts["datex"] >= start]

        # Drop the 'type' column now that we've filtered AM_wy
        am_wy = am_wy.drop(columns=["type"])

        # Set as indexes once here
        am_wy = am_wy.set_index("date").sort_index()
        counts = counts.set_index(["datex", "group_name"]).sort_index()

        # Keep references for debugging and introspection
        self.am_wy = am_wy
        self.counts = counts

        group_data = self._build_group_data(counts, am_wy)

        # DEBUG: run for the date of interest regardless of mismatches
        self.debug_group_assignment_for_date("2026-08-27")

        self.group_frames = self._to_sheet_frames(group_data)

    def _AM_wy_and_group_count_query(self):
        with self.engine.connect() as conn:
            am_wy = pd.read_sql_table('AM_wy', conn)
            group_counts = pd.read_sql_table('group_counts', conn)
        return am_wy, group_counts

    def _build_group_data(self, counts_df: pd.DataFrame, wide_df: pd.DataFrame) -> dict:
        """
        Returns {group_name: {date: [wy_id, wy_id, ...]}} by slicing each
        day's ordered wy_id sequence (from AM_wy's c1, c2, ... columns)
        according to that day's group_counts, in GROUP_ORDER.
        """
        c_cols = sorted(
            [c for c in wide_df.columns if c.lower().startswith("c") and c[1:].isdigit()],
            key=lambda c: int(c[1:])
        )

        # Build a fast Series lookup: MultiIndex (datex, group_name) -> count
        counts_lookup = counts_df.sort_index()["count"]

        group_data = {g: {} for g in self.GROUP_ORDER}

        for date, row in wide_df.iterrows():
            values = [row[c] for c in c_cols if pd.notna(row[c])]

            if not values:
                # Keep the date as an all-NaN column instead of dropping it
                for g in self.GROUP_ORDER:
                    group_data[g][date] = []
                continue

            try:
                today_counts = [int(counts_lookup.loc[(date, g)]) for g in self.GROUP_ORDER]
            except KeyError:
                # print(f"SKIP {date.date()}: missing one or more group_counts rows")
                continue

            if sum(today_counts) != len(values):
                # print(
                #     f"WARNING {date.date()}: counts sum to {sum(today_counts)} "
                #     f"but {len(values)} wy_ids present — skipping this date"
                # )
                continue

            pos = 0
            for g, n in zip(self.GROUP_ORDER, today_counts):
                group_data[g][date] = values[pos:pos + n]
                pos += n

        return group_data

    def debug_group_assignment_for_date(self, date):
        """
        Builds a debug DataFrame for a given date, concatenating wy_ids
        from AM_wy using group_counts, regardless of length mismatches.
        Result is stored as self.debug_df (wide, single-row).
        """
        date = pd.Timestamp(date).normalize()

        if date not in self.am_wy.index:
            self.debug_df = pd.DataFrame({"error": [f"{date.date()} missing from am_wy"]})
            return

        row = self.am_wy.loc[date]
        c_cols = sorted(
            [c for c in self.am_wy.columns if c.lower().startswith("c") and c[1:].isdigit()],
            key=lambda c: int(c[1:])
        )
        wy_ids = [row[c] for c in c_cols if pd.notna(row[c])]

        # Retrieve counts for GROUP_ORDER
        today_counts = []
        missing_groups = []
        for g in self.GROUP_ORDER:
            try:
                today_counts.append(int(self.counts.loc[(date, g), "count"]))
            except KeyError:
                missing_groups.append(g)
                today_counts.append(None)

        counts_sum = sum(c for c in today_counts if c is not None)

        # Build concatenated list using available wy_ids, regardless of mismatch
        pos = 0
        group_slices = {}
        assigned_ids = []
        for g, n in zip(self.GROUP_ORDER, today_counts):
            n = 0 if n is None else n
            slice_ids = wy_ids[pos:pos + n]
            group_slices[g] = slice_ids
            assigned_ids.extend(slice_ids)
            pos += n

        # Determine discrepancy
        if counts_sum > len(wy_ids):
            discrepancy = f"Deficit: {counts_sum - len(wy_ids)} wy_id(s) missing from AM_wy"
            extra_ids = []
        elif counts_sum < len(wy_ids):
            discrepancy = f"Extra: {len(wy_ids) - counts_sum} wy_id(s) in AM_wy not covered by any group"
            extra_ids = wy_ids[counts_sum:]
        else:
            discrepancy = "Counts match AM_wy values exactly."
            extra_ids = []

        # Build a single-row, wide DataFrame for the debug viewer
        debug_data = {
            "date": date,
            "am_wy_count": len(wy_ids),
            "counts_sum": counts_sum,
            "discrepancy": discrepancy,
            "extra_ids": extra_ids,
            "assigned_ids": assigned_ids,
        }
        # Add per-group count and assigned list columns
        for g in self.GROUP_ORDER:
            debug_data[f"{g}_count"] = today_counts[self.GROUP_ORDER.index(g)]
            debug_data[f"{g}_assigned"] = group_slices[g]

        self.debug_df = pd.DataFrame([debug_data])
        
        
    def _to_sheet_frames(self, group_data: dict) -> dict:
        frames = {}
        for g, date_map in group_data.items():
            if not date_map:
                frames[g] = pd.DataFrame()
                continue

            max_len = max([len(v) for v in date_map.values()] + [1])
            dates_sorted = sorted(date_map.keys())

            # Build a continuous daily index so columns are chronological
            # and missing dates appear as all-NaN columns.
            all_dates = pd.date_range(dates_sorted[0], dates_sorted[-1], freq="D")
            data = {
                d.strftime("%Y-%m-%d"): date_map.get(d, []) + [None] * (max_len - len(date_map.get(d, [])))
                for d in all_dates
            }

            df = pd.DataFrame(data)
            df.index = range(1, max_len + 1)
            df.index.name = "index"
            frames[g] = df

        return frames

    def split_group_frames(self):
        """
        Unpacks self.group_frames into individually named DataFrame
        attributes, e.g. self.fresh_df, self.group_A_df, self.group_B_df,
        self.group_C_df, self.sick_df.
        """
        self.fresh_df   = self.group_frames.get("fresh",   pd.DataFrame())
        self.group_A_df = self.group_frames.get("group_A", pd.DataFrame())
        self.group_B_df = self.group_frames.get("group_B", pd.DataFrame())
        self.group_C_df = self.group_frames.get("group_C", pd.DataFrame())
        self.sick_df    = self.group_frames.get("sick",    pd.DataFrame())


if __name__ == '__main__':
    obj = BuildWBGroups()
    obj.load()