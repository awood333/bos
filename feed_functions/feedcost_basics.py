"""feed_functions\\feedcost_basics.py"""

import inspect
import pandas as pd
from sqlalchemy import text
from sql_db_related.neon_connect import get_engine
from container import get_dependency


class DatabaseDataLoader:
    """Handles structured extraction of feed ledger entries directly from Neon Postgres."""

    def __init__(self):
        self.engine = get_engine()

    def get_feed_daily_amt_last_rows(self) -> pd.DataFrame:
        """
        Retrieves the 'last known values per feed type' table -- one row
        per canonical feed type, kept correct via the gdrive-sourced
        LastRows sheet. feed_type here is already clean text (no CP_
        prefix, no _daily_amt suffix).
        """
        with self.engine.connect() as conn:
            last_rows_table = pd.read_sql_table("feed_daily_amt_last_rows", con=conn)
        return last_rows_table

    def get_canonical_feed_types(self) -> list:
        """
        Derives the canonical feed_type list from feed_daily_amt_last_rows
        rather than a hardcoded list -- so adding/retiring a feed type
        doesn't need a code change, just an update to the gdrive sheet.
        """
        last_rows_table = self.get_feed_daily_amt_last_rows()
        return sorted(last_rows_table["feed_type"].dropna().str.strip().unique().tolist())

    def load_invoice_prices(self, feed: str) -> pd.DataFrame:
        """Retrieves historical unit pricing sequences for an isolated feed type."""
        query = text("""
            SELECT invoice_date, price_per_kg
            FROM feed_invoice_ledger
            WHERE feed_type = :feed_type
            ORDER BY invoice_date
        """)
        with self.engine.connect() as conn:
            invoice_prices_table = pd.read_sql(query, conn, params={"feed_type": feed})

        invoice_prices_table.columns = invoice_prices_table.columns.str.strip()
        invoice_prices_table["invoice_date"] = pd.to_datetime(
            invoice_prices_table["invoice_date"], errors="coerce")
        invoice_prices_table = invoice_prices_table.dropna(subset=["invoice_date"])

        invoice_prices_table = invoice_prices_table.set_index("invoice_date")[["price_per_kg"]].rename(
            columns={"price_per_kg": "unit_price"})
        invoice_prices_table["unit_price"] = pd.to_numeric(
            invoice_prices_table["unit_price"], errors="coerce")

        return invoice_prices_table[~invoice_prices_table.index.duplicated(keep="last")].sort_index()

    def load_daily_amounts(self, feed: str) -> pd.DataFrame:
        """Retrieves historical volumetric feed distribution weights for an isolated feed type."""
        query = text("""
            SELECT datex, fresh_kg, group_a_kg, group_b_kg, group_c_kg, dry_kg
            FROM feed_daily_amt_ledger
            WHERE feed_type = :feed_type
            ORDER BY datex
        """)
        with self.engine.connect() as conn:
            daily_amounts_table = pd.read_sql(query, conn, params={"feed_type": feed})

        daily_amounts_table.columns = daily_amounts_table.columns.str.strip()
        daily_amounts_table["datex"] = pd.to_datetime(daily_amounts_table["datex"], errors="coerce")
        daily_amounts_table = daily_amounts_table.dropna(subset=["datex"])

        daily_amounts_table = daily_amounts_table.set_index("datex")
        daily_amounts_table = daily_amounts_table.apply(pd.to_numeric, errors="coerce")

        return daily_amounts_table[~daily_amounts_table.index.duplicated(keep="last")].sort_index()
        # heifer_kg intentionally excluded for now -- comes back as a 6th
        # group once heifer feedcost handling is defined.

    def get_iu_merge(self) -> pd.DataFrame:
        """
        Pulls the entire iu_merge table -- insemination/ultrasound/stop/
        birth events, one row per event, already long-format. Only
        covers currently-alive wy_ids by construction (dead cows aren't
        in this table), which is fine since we don't need feedcost
        history for cows no longer being fed. If an alive_mask is
        applied elsewhere to self.feed_types-adjacent per-cow data,
        it's redundant here -- this table is pre-filtered to alive cows
        already, not something this function needs to filter itself.

        Not used yet -- the B/C pregnancy split (which cows are dry vs
        low-yield-but-not-pregnant vs low-yield-and-pregnant) is a
        separate calculation layered on top of feedcost_basics, not
        part of it. This just gets the raw data in hand.
        """
        query = text("SELECT * FROM iu_merge ORDER BY wy_id, datex")
        with self.engine.connect() as conn:
            iu_merge_table = pd.read_sql(query, conn)

        iu_merge_table.columns = iu_merge_table.columns.str.strip()
        iu_merge_table["datex"] = pd.to_datetime(iu_merge_table["datex"], errors="coerce")
        iu_merge_table["stop_date"] = pd.to_datetime(iu_merge_table["stop_date"], errors="coerce")

        return iu_merge_table


class FeedcostBasics:
    """
    Computes the daily feed cost per group (F=fresh, A, B, C, D=dry):
    for each feed type, on each day, cost = price_per_kg(that day) x
    kg_used(that day); summed across all feed types for a given group
    column gives that group's total daily feed cost. That's the whole
    job -- weekly/monthly rollups and the B/C pregnancy split both live
    elsewhere, not here.
    """

    def __init__(self):
        print(f"FeedcostBasics instantiated by: {inspect.stack()[1].filename}")
        self.db_loader = DatabaseDataLoader()

        self.milk_basics = None
        self.date_range = None
        self.rng_daily = None

        # Canonical active inventory types -- populated in load_and_process
        # from feed_daily_amt_last_rows rather than hardcoded here.
        self.feed_types = None

        self.price_seq_dict = {}
        self.daily_amt_dict = {}

        # One daily cost DataFrame per group -- each has one column per
        # feed type plus a 'totalcost{X}' sum column.
        self.feedcost_F_df = None
        self.feedcost_A_df = None
        self.feedcost_B_df = None
        self.feedcost_C_df = None
        self.feedcost_D_df = None

        self.iu_merge_df = None

    def load_and_process(self):
        """Orchestrates dependency loading and daily feedcost computation."""
        self.milk_basics = get_dependency("milk_basics")
        self.date_range = get_dependency("date_range")
        self.rng_daily = self.date_range.date_range_daily

        self.feed_types = self.db_loader.get_canonical_feed_types()

        for feed in self.feed_types:
            raw_prices = self.db_loader.load_invoice_prices(feed)
            raw_amounts = self.db_loader.load_daily_amounts(feed)

            self.price_seq_dict[feed] = raw_prices.reindex(self.rng_daily, method='ffill')
            self.daily_amt_dict[feed] = raw_amounts.reindex(self.rng_daily, method='ffill')

        self._calculate_all_group_costs()

        # raw data only -- not yet used for the B/C split
        self.iu_merge_df = self.db_loader.get_iu_merge()

    def _calculate_isolated_group_costs(self, kg_column: str) -> dict:
        """
        Daily cost per feed = price_per_kg(that day) x kg_used(that day),
        for one consumption column (e.g. group_a_kg). Vectorized across
        the whole date range per feed, rather than looping date-by-date.
        """
        daily_cost_dict = {}
        for feed in self.feed_types:
            unit_price_series = self.price_seq_dict[feed]['unit_price']
            kg_series = self.daily_amt_dict[feed][kg_column]
            daily_cost_dict[feed] = (unit_price_series.fillna(0) * kg_series.fillna(0)).values
        return daily_cost_dict

    def _compile_group_dataframe(self, cost_dict: dict, group_name: str) -> pd.DataFrame:
        """One column per feed's daily cost, plus a totalcost{group_name} sum column."""
        group_cost_table = pd.DataFrame({f: pd.Series(cost_dict[f]).astype(float) for f in self.feed_types})
        group_cost_table = group_cost_table.fillna(0)
        group_cost_table[f'totalcost{group_name}'] = group_cost_table.sum(axis=1)
        group_cost_table.index = self.rng_daily
        return group_cost_table

    def _calculate_all_group_costs(self):
        """Daily feed cost, per feed and total, for each of the 4 groups (heifers deferred)."""
        self.feedcost_F_df = self._compile_group_dataframe(self._calculate_isolated_group_costs('fresh_kg'), 'F')
        self.feedcost_A_df = self._compile_group_dataframe(self._calculate_isolated_group_costs('group_a_kg'), 'A')
        self.feedcost_B_df = self._compile_group_dataframe(self._calculate_isolated_group_costs('group_b_kg'), 'B')
        self.feedcost_C_df = self._compile_group_dataframe(self._calculate_isolated_group_costs('group_c_kg'), 'C')
        self.feedcost_D_df = self._compile_group_dataframe(self._calculate_isolated_group_costs('dry_kg'), 'D')


if __name__ == "__main__":
    processor = FeedcostBasics()
    processor.load_and_process()
    print("Feedcost basics computed: daily cost per feed, per group (F/A/B/C/D).")