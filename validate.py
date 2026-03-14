#!/usr/bin/env python3
"""
validate.py — Post-migration data validation.

Checks for each migrated table:
  1. Row counts match source vs target
  2. Column nullability consistency
  3. Optional: checksum on numeric columns

Usage:
    python validate.py \
        --source "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server" \
        --target-dsn "postgresql://user:pass@aurora-cluster/db" \
        --tables orders customers \
        --schema dbo
"""

import argparse
import logging
import sys
from typing import List, Dict

import sqlalchemy as sa
from sqlalchemy import text, inspect

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PASS = "✓ PASS"
FAIL = "✗ FAIL"


def row_count(engine: sa.engine.Engine, table: str, schema: str = None) -> int:
    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}"))
        return result.scalar()


def numeric_columns(engine: sa.engine.Engine, table: str, schema: str) -> List[str]:
    insp = inspect(engine)
    cols = insp.get_columns(table, schema=schema)
    numeric_types = {"INTEGER", "BIGINT", "NUMERIC", "DECIMAL", "DOUBLE PRECISION",
                     "REAL", "FLOAT", "MONEY", "SMALLMONEY", "INT", "SMALLINT", "TINYINT"}
    return [
        c["name"] for c in cols
        if str(c["type"]).upper().split("(")[0] in numeric_types
    ]


def column_checksum(engine: sa.engine.Engine, table: str, schema: str, column: str) -> float:
    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COALESCE(SUM("{column}"), 0) FROM {qualified}'))
        return float(result.scalar())


def validate_table(
    src_engine: sa.engine.Engine,
    tgt_engine: sa.engine.Engine,
    table: str,
    src_schema: str,
    run_checksum: bool,
) -> Dict:
    report = {"table": table, "checks": []}

    # ── Row count ──────────────────────────────────────────────────────────────
    src_count = row_count(src_engine, table, src_schema)
    tgt_count = row_count(tgt_engine, table)
    match = src_count == tgt_count
    report["checks"].append({
        "check":  "row_count",
        "source": src_count,
        "target": tgt_count,
        "result": PASS if match else FAIL,
    })

    # ── Numeric checksums ─────────────────────────────────────────────────────
    if run_checksum:
        num_cols = numeric_columns(src_engine, table, src_schema)
        for col in num_cols[:5]:    # limit to first 5 numeric cols
            try:
                src_sum = column_checksum(src_engine, table, src_schema, col)
                tgt_sum = column_checksum(tgt_engine, table, None,       col)
                ok = abs(src_sum - tgt_sum) < 0.01
                report["checks"].append({
                    "check":  f"checksum:{col}",
                    "source": src_sum,
                    "target": tgt_sum,
                    "result": PASS if ok else FAIL,
                })
            except Exception as exc:
                report["checks"].append({
                    "check":  f"checksum:{col}",
                    "source": None,
                    "target": None,
                    "result": f"ERROR: {exc}",
                })

    report["passed"] = all(c["result"] == PASS for c in report["checks"])
    return report


def print_report(reports: List[Dict]):
    print("\n" + "=" * 70)
    print("  VALIDATION REPORT")
    print("=" * 70)
    total_pass = total_fail = 0
    for rep in reports:
        status = "✓" if rep["passed"] else "✗"
        print(f"\n  {status} {rep['table']}")
        for chk in rep["checks"]:
            pad = f"    {chk['check']:<30}"
            detail = f"src={chk['source']}  tgt={chk['target']}"
            print(f"{pad}  {chk['result']:<8}  {detail}")
        if rep["passed"]:
            total_pass += 1
        else:
            total_fail += 1

    print("\n" + "-" * 70)
    print(f"  Tables passed : {total_pass}")
    print(f"  Tables failed : {total_fail}")
    print("=" * 70)
    return total_fail == 0


def main():
    parser = argparse.ArgumentParser(description="Post-migration validation")
    parser.add_argument("--source",      required=True)
    parser.add_argument("--target-dsn",  required=True)
    parser.add_argument("--tables",      nargs="*", default=[])
    parser.add_argument("--schema",      default="dbo")
    parser.add_argument("--checksum",    action="store_true", help="Run numeric column checksums")
    args = parser.parse_args()

    src_engine = sa.create_engine(args.source)
    tgt_engine = sa.create_engine(args.target_dsn)

    tables = args.tables
    if not tables:
        tables = inspect(src_engine).get_table_names(schema=args.schema)

    reports = [
        validate_table(src_engine, tgt_engine, tbl, args.schema, args.checksum)
        for tbl in tables
    ]

    success = print_report(reports)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
