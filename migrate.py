#!/usr/bin/env python3
"""
migrate.py — SQL Server → Cloud ETL Orchestrator
Supports: Amazon Aurora (PostgreSQL), Azure SQL, Google AlloyDB

Usage:
    python migrate.py --source "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server" \
                      --target aurora \
                      --target-dsn "postgresql://user:pass@aurora-cluster/db" \
                      --tables orders customers products \
                      --batch-size 5000

Targets: aurora | azure | alloydb
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from etl_core import ETLPipeline
from cloud_targets import get_target_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="SQL Server → Cloud ETL Migration")
    parser.add_argument("--source",      required=True,  help="SQLAlchemy DSN for source SQL Server")
    parser.add_argument("--target",      required=True,  choices=["aurora", "azure", "alloydb"],
                        help="Cloud target platform")
    parser.add_argument("--target-dsn",  required=True,  help="SQLAlchemy DSN for cloud target")
    parser.add_argument("--tables",      nargs="*",       help="Tables to migrate (default: all)")
    parser.add_argument("--schema",      default="dbo",   help="Source schema (default: dbo)")
    parser.add_argument("--batch-size",  type=int, default=5000, help="Rows per batch (default: 5000)")
    parser.add_argument("--workers",     type=int, default=4,    help="Parallel table workers (default: 4)")
    parser.add_argument("--dry-run",     action="store_true",    help="Preview only, no writes")
    parser.add_argument("--truncate",    action="store_true",    help="Truncate target tables before load")
    return parser.parse_args()


def main():
    args = parse_args()

    log.info("=" * 60)
    log.info(f"  SQL Server → {args.target.upper()} Migration")
    log.info(f"  Batch size : {args.batch_size:,}")
    log.info(f"  Workers    : {args.workers}")
    log.info(f"  Dry run    : {args.dry_run}")
    log.info("=" * 60)

    target_engine = get_target_engine(args.target, args.target_dsn)

    pipeline = ETLPipeline(
        source_dsn=args.source,
        target_engine=target_engine,
        schema=args.schema,
        batch_size=args.batch_size,
        workers=args.workers,
        dry_run=args.dry_run,
        truncate=args.truncate,
    )

    tables = args.tables or pipeline.discover_tables()
    log.info(f"Tables queued: {len(tables)}  →  {tables}")

    start = time.time()
    results = pipeline.run(tables)
    elapsed = time.time() - start

    # ── Summary ──────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("  MIGRATION SUMMARY")
    log.info("=" * 60)
    total_rows = 0
    errors = []
    for tbl, info in results.items():
        status = "✓" if info["success"] else "✗"
        log.info(f"  {status}  {tbl:<40} {info['rows_migrated']:>10,} rows")
        total_rows += info["rows_migrated"]
        if not info["success"]:
            errors.append((tbl, info.get("error")))

    log.info("-" * 60)
    log.info(f"  Total rows : {total_rows:,}")
    log.info(f"  Elapsed    : {elapsed:.1f}s")
    log.info(f"  Throughput : {total_rows / max(elapsed, 1):,.0f} rows/sec")
    if errors:
        log.warning(f"\n  FAILED TABLES ({len(errors)}):")
        for tbl, err in errors:
            log.warning(f"    • {tbl}: {err}")
        sys.exit(1)
    else:
        log.info("\n  All tables migrated successfully ✓")


if __name__ == "__main__":
    main()
