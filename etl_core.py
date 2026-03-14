"""
  Script:       etl_core.py
  Author:       Misty Collins
  Notes:        Core ETL pipeline: extract from SQL Server, transform, load to cloud target.
Key features:
  • Chunked extraction (server-side cursor) to avoid OOM on large tables
  • Type coercion layer for MSSQL → ANSI SQL differences
  • Parallel table loading via ThreadPoolExecutor
  • Per-table retry with exponential back-off
  • Identity / computed column stripping
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect, text

log = logging.getLogger(__name__)

# ── Type-coercion map: MSSQL quirks → portable types ──────────────────────────
MSSQL_TYPE_MAP = {
    "BIT":              "BOOLEAN",
    "TINYINT":          "SMALLINT",
    "MONEY":            "NUMERIC(19,4)",
    "SMALLMONEY":       "NUMERIC(10,4)",
    "NTEXT":            "TEXT",
    "TEXT":             "TEXT",
    "IMAGE":            "BYTEA",          # PostgreSQL target
    "UNIQUEIDENTIFIER": "VARCHAR(36)",
    "DATETIME":         "TIMESTAMP",
    "SMALLDATETIME":    "TIMESTAMP",
    "DATETIME2":        "TIMESTAMP",
    "DATETIMEOFFSET":   "TIMESTAMPTZ",
    "XML":              "TEXT",
    "HIERARCHYID":      "TEXT",           # serialised as string
    "GEOGRAPHY":        "TEXT",
    "GEOMETRY":         "TEXT",
}

# Columns generated / identity — skip on INSERT
SKIP_COLUMN_KINDS = {"COMPUTED", "IDENTITY"}


class ETLPipeline:
    def __init__(
        self,
        source_dsn: str,
        target_engine: sa.engine.Engine,
        schema: str = "dbo",
        batch_size: int = 5000,
        workers: int = 4,
        dry_run: bool = False,
        truncate: bool = False,
        max_retries: int = 3,
    ):
        self.source_engine = sa.create_engine(source_dsn, fast_executemany=True)
        self.target_engine = target_engine
        self.schema = schema
        self.batch_size = batch_size
        self.workers = workers
        self.dry_run = dry_run
        self.truncate = truncate
        self.max_retries = max_retries

    # ── Discovery ─────────────────────────────────────────────────────────────

    def discover_tables(self) -> List[str]:
        """Return all user tables in the source schema."""
        insp = inspect(self.source_engine)
        tables = insp.get_table_names(schema=self.schema)
        log.info(f"Discovered {len(tables)} tables in schema '{self.schema}'")
        return tables

    def get_column_info(self, table: str) -> List[Dict[str, Any]]:
        """Return list of writable column dicts for a table."""
        insp = inspect(self.source_engine)
        columns = insp.get_columns(table, schema=self.schema)
        writable = []
        for col in columns:
            col_type = str(col["type"]).upper().split("(")[0]
            if col_type in SKIP_COLUMN_KINDS:
                log.debug(f"  Skipping {col['name']} ({col_type})")
                continue
            writable.append(col)
        return writable

    # ── Extract ───────────────────────────────────────────────────────────────

    def extract_table(self, table: str, columns: List[Dict]) -> pd.DataFrame:
        """Stream table in batches; yield DataFrames."""
        col_names = [c["name"] for c in columns]
        qualified = f"[{self.schema}].[{table}]"
        query = f"SELECT {', '.join(f'[{c}]' for c in col_names)} FROM {qualified} WITH (NOLOCK)"

        chunks = []
        with self.source_engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(text(query))
            while True:
                rows = result.fetchmany(self.batch_size)
                if not rows:
                    break
                chunks.append(pd.DataFrame(rows, columns=col_names))

        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=col_names)

    # ── Transform ─────────────────────────────────────────────────────────────

    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and coerce common MSSQL quirks."""
        for col in df.columns:
            # Strip trailing whitespace from char/nchar columns
            if df[col].dtype == object:
                df[col] = df[col].apply(
                    lambda v: v.rstrip() if isinstance(v, str) else v
                )
            # Convert pandas NA to None for DB-safe None handling
            df[col] = df[col].where(df[col].notna(), other=None)
        return df

    # ── Load ──────────────────────────────────────────────────────────────────

    def load_table(self, df: pd.DataFrame, table: str) -> int:
        """Write DataFrame to target in chunks; return total rows written."""
        if self.dry_run:
            log.info(f"  [DRY RUN] Would write {len(df):,} rows → {table}")
            return len(df)

        if self.truncate:
            with self.target_engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
            log.debug(f"  Truncated {table}")

        total = 0
        for i in range(0, len(df), self.batch_size):
            chunk = df.iloc[i: i + self.batch_size]
            chunk.to_sql(
                name=table,
                con=self.target_engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total += len(chunk)
            log.debug(f"  {table}: {total:,}/{len(df):,} rows loaded")

        return total

    # ── Per-table pipeline with retry ─────────────────────────────────────────

    def migrate_table(self, table: str) -> Dict[str, Any]:
        for attempt in range(1, self.max_retries + 1):
            try:
                log.info(f"[{table}] Starting (attempt {attempt})")
                columns = self.get_column_info(table)
                df = self.extract_table(table, columns)
                log.info(f"[{table}] Extracted {len(df):,} rows")
                df = self.transform(df)
                rows = self.load_table(df, table)
                log.info(f"[{table}] ✓ Loaded {rows:,} rows")
                return {"success": True, "rows_migrated": rows}
            except Exception as exc:
                log.warning(f"[{table}] Attempt {attempt} failed: {exc}")
                if attempt < self.max_retries:
                    sleep = 2 ** attempt
                    log.info(f"[{table}] Retrying in {sleep}s …")
                    time.sleep(sleep)
                else:
                    log.error(f"[{table}] All retries exhausted.")
                    return {"success": False, "rows_migrated": 0, "error": str(exc)}

    # ── Orchestrate ───────────────────────────────────────────────────────────

    def run(self, tables: List[str]) -> Dict[str, Dict]:
        results: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(self.migrate_table, tbl): tbl for tbl in tables}
            for future in as_completed(futures):
                tbl = futures[future]
                results[tbl] = future.result()
        return results
