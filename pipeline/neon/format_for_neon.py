import re
import pandas as pd
from sqlalchemy import text


class FormatForNeon:
    """
    Generic Modal-side type coercion + safe write for a DataFrame going to Neon.

    Two ways to type columns:
    - `schema`: {column_name: kind} for columns with stable, known names
      (kind in 'int' | 'float' | 'date' | 'text' | 'bool')
    - `positional_rules`: [(start_idx, end_idx, kind), ...] for columns
      whose *names* change per run (e.g. tenday's rolling
      10-day date columns) but whose *position* is fixed.
      Applied by df.columns[start_idx:end_idx], not by name.

    Only handles type fidelity (dtype + real None for missing).
    Display formatting stays in bos_dashboard.
    """

    _PG_TYPE_MAP = {
        "integer": "int", "bigint": "int", "smallint": "int",
        "text": "text", "varchar": "text", "char": "text",
        "date": "date", "timestamp": "date", "timestamptz": "date",
        "double precision": "float", "real": "float",
        "numeric": "float", "float": "float",
        "boolean": "bool",
    }

    def __init__(self, schema: dict = None, positional_rules: list = None):
        """
        schema: {column_name: pg_type_or_kind}. Accepts either raw Postgres
        type strings ("INTEGER", "TEXT", "DATE") or already-normalized
        kinds ("int", "text", "date") — both are handled.
        positional_rules: list of (start_idx, end_idx, kind) tuples, applied
        in order after the named schema. end_idx is exclusive, matching
        Python slice convention (unlike the old range(1, 11) code, which
        is idx 1..10 inclusive -> here that's (1, 11, 'int')).
        """
        self.spec = {}
        for col, pg_type in (schema or {}).items():
            self.spec[col] = self._normalize_kind(pg_type)
        self.positional_rules = positional_rules or []

    def _normalize_kind(self, pg_type: str) -> str:
        key = pg_type.strip().lower()
        key = re.sub(r"\s+primary key$", "", key)
        key = re.sub(r"\s+not null$", "", key)
        if key in {"int", "float", "date", "text", "bool"}:
            return key
        return self._PG_TYPE_MAP.get(key, None)

    @classmethod
    def from_ddl_block(cls, ddl_text: str, positional_rules: list = None):
        """
        Build from a pasted Neon schema block, one 'col_name TYPE...' per
        line — works whether it's copied from the STRUCTURE tab column
        list or from an information_schema query result.
        """
        schema = {}
        for line in ddl_text.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = re.split(r"[\s,|]+", line, maxsplit=1)
            if len(parts) == 2:
                schema[parts[0]] = parts[1]
        return cls(schema=schema, positional_rules=positional_rules)

    @classmethod
    def from_information_schema(cls, table_name: str, engine, positional_rules: list = None):
        """
        Skip copy-paste entirely: query Neon directly for the current
        column list and types. Preferred over from_ddl_block when you
        have a live engine handy.
        """
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = :t ORDER BY ordinal_position"
            ), {"t": table_name}).fetchall()
        schema = {r[0]: r[1] for r in rows}
        return cls(schema=schema, positional_rules=positional_rules)

    def _kind_for_positional(self, col_idx: int):
        for start, end, kind in self.positional_rules:
            if start <= col_idx < end:
                return kind
        return None

    def _coerce_column(self, series: pd.Series, kind: str) -> pd.Series:
        if kind == "date":
            return pd.to_datetime(series, errors="coerce")
        elif kind == "int":
            return pd.to_numeric(series, errors="coerce").round(0).astype("Int64")
        elif kind == "float":
            return pd.to_numeric(series, errors="coerce")
        elif kind == "bool":
            return series.astype("boolean")
        elif kind == "text":
            out = series.astype(str)
            return out.replace(["nan", "NaN", "None", "<NA>"], None)
        else:
            # No spec: leave numeric alone; swap NaN-likes to None on object cols.
            if not pd.api.types.is_numeric_dtype(series):
                return series.where(series.notna(), None)
            return series

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Type-coerce a normal (non date-indexed) table."""
        out = df.copy()
        for idx, col in enumerate(out.columns):
            kind = self.spec.get(col) or self._kind_for_positional(idx)
            out[col] = self._coerce_column(out[col], kind)
        return out

    def apply_indexed_date(self, df: pd.DataFrame, index_name: str = "date") -> pd.DataFrame:
        """
        For wide tables like `fullday` where the date lives in the index
        (e.g. MAB.fullday_calc's 'datex'). Value columns are left as
        native float unless named/positioned in schema/positional_rules.
        """
        out = df.copy()
        out.index = pd.to_datetime(out.index, errors="coerce")
        out.index.name = index_name
        out = out.reset_index()
        # re-apply column typing to everything except the new date column
        for idx, col in enumerate(out.columns):
            if col == index_name:
                continue
            kind = self.spec.get(col) or self._kind_for_positional(idx)
            out[col] = self._coerce_column(out[col], kind)
        return out

    def _pk_clause(self, pk_col):
        """
        Build a SQL-safe column list for PRIMARY KEY / ON CONFLICT targets.
        Accepts a single column name (str) or multiple (list/tuple) for
        composite keys.
        """
        if isinstance(pk_col, (list, tuple)):
            return ", ".join(f'"{c}"' for c in pk_col)
        return f'"{pk_col}"'

    def write(self, df: pd.DataFrame, table_name: str, engine, pk_col=None,
              indexed_date: bool = False, date_index_name: str = "date"):
        """
        Type-coerce and write using a fresh engine connection/transaction.
        Re-adds the PRIMARY KEY every time — to_sql(if_exists='replace')
        always drops it otherwise. Use this for standalone, one-off writes.

        pk_col: single column name (str), or a list/tuple of column names
        for a composite primary key.
        """
        typed = (self.apply_indexed_date(df, date_index_name)
                 if indexed_date else self.apply(df))

        with engine.begin() as conn:
            typed.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"[neon] {table_name} written: {typed.shape}")

            if pk_col:
                conn.execute(text(
                    f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({self._pk_clause(pk_col)})'
                ))

        return typed

    def write_conn(self, df: pd.DataFrame, table_name: str, conn, pk_col=None,
                   indexed_date: bool = False, date_index_name: str = "date"):
        """
        Same as write(), but takes an already-open connection instead of
        an engine, so multiple tables can be written inside one shared
        engine.begin() block (all commit or roll back together).

        pk_col: single column name (str), or a list/tuple of column names
        for a composite primary key.
        """
        typed = (self.apply_indexed_date(df, date_index_name)
                 if indexed_date else self.apply(df))

        typed.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"[neon] {table_name} written: {typed.shape}")

        if pk_col:
            conn.execute(text(
                f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({self._pk_clause(pk_col)})'
            ))

        return typed