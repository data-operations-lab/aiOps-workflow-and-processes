#!/usr/bin/env python3
"""
  Script:        cloud_targets.py
  Author:        Misty Collins
  Notes:         Engine factory for cloud databases


Generates CREATE TABLE statements adjusted for each cloud target:
  • Aurora / AlloyDB (PostgreSQL) : MSSQL types → PG types, identity → SERIAL/BIGSERIAL
  • Azure SQL (T-SQL)             : mostly compatible, strips unsupported hints

Usage:
    python schema_export.py \
        --source "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server" \
        --target aurora \
        --schema dbo \
        --output schema_aurora.sql
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List

import sqlalchemy as sa
from sqlalchemy import inspect, text

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Type mapping: MSSQL → PostgreSQL ─────────────────────────────────────────
MSSQL_TO_PG = {
    "BIT":              "BOOLEAN",
    "TINYINT":          "SMALLINT",
    "SMALLINT":         "SMALLINT",
    "INT":              "INTEGER",
    "BIGINT":           "BIGINT",
    "FLOAT":            "DOUBLE PRECISION",
    "REAL":             "REAL",
    "MONEY":            "NUMERIC(19,4)",
    "SMALLMONEY":       "NUMERIC(10,4)",
    "DECIMAL":          "NUMERIC",
    "NUMERIC":          "NUMERIC",
    "CHAR":             "CHAR",
    "NCHAR":            "CHAR",
    "VARCHAR":          "VARCHAR",
    "NVARCHAR":         "VARCHAR",
    "TEXT":             "TEXT",
    "NTEXT":            "TEXT",
    "BINARY":           "BYTEA",
    "VARBINARY":        "BYTEA",
    "IMAGE":            "BYTEA",
    "DATE":             "DATE",
    "TIME":             "TIME",
    "DATETIME":         "TIMESTAMP",
    "DATETIME2":        "TIMESTAMP",
    "SMALLDATETIME":    "TIMESTAMP",
    "DATETIMEOFFSET":   "TIMESTAMPTZ",
    "UNIQUEIDENTIFIER": "UUID",
    "XML":              "TEXT",
    "HIERARCHYID":      "TEXT",
    "GEOGRAPHY":        "TEXT",
    "GEOMETRY":         "TEXT",
}

# ── Type mapping: MSSQL → Azure SQL (T-SQL, mostly pass-through) ──────────────
MSSQL_TO_AZURE = {
    "NTEXT":    "NVARCHAR(MAX)",
    "TEXT":     "VARCHAR(MAX)",
    "IMAGE":    "VARBINARY(MAX)",
    # everything else stays the same
}


def pg_column_def(col, is_identity: bool = False) -> str:
    raw_type = str(col["type"]).upper()
    base = raw_type.split("(")[0].strip()
    length_part = ""
    if "(" in raw_type:
        length_part = "(" + raw_type.split("(", 1)[1]

    pg_base = MSSQL_TO_PG.get(base, base)

    if is_identity:
        pg_type = "BIGSERIAL" if base == "BIGINT" else "SERIAL"
        nullable = ""
    else:
        pg_type = f"{pg_base}{length_part}" if length_part else pg_base
        nullable = "" if col.get("nullable", True) else " NOT NULL"

    default = ""
    if col.get("default") and not is_identity:
        default = f" DEFAULT {col['default']}"

    return f'  "{col["name"]}" {pg_type}{nullable}{default}'


def azure_column_def(col, is_identity: bool = False) -> str:
    raw_type = str(col["type"]).upper()
    base = raw_type.split("(")[0].strip()
    length_part = ""
    if "(" in raw_type:
        length_part = "(" + raw_type.split("(", 1)[1]

    az_base = MSSQL_TO_AZURE.get(base, base)
    az_type = f"{az_base}{length_part}" if length_part else az_base
    identity = " IDENTITY(1,1)" if is_identity else ""
    nullable = "" if col.get("nullable", True) else " NOT NULL"
    default = ""
    if col.get("default") and not is_identity:
        default = f" DEFAULT {col['default']}"

    return f"  [{col['name']}] {az_type}{identity}{nullable}{default}"


def export_schema(
    source_dsn: str,
    schema: str,
    target: str,
    tables: List[str],
    output_path: Path,
):
    engine = sa.create_engine(source_dsn)
    insp = inspect(engine)

    if not tables:
        tables = insp.get_table_names(schema=schema)
    log.info(f"Exporting {len(tables)} tables → {target.upper()} DDL")

    lines = [
        f"-- DDL generated for target: {target.upper()}",
        f"-- Source schema: {schema}",
        f"-- Tables: {len(tables)}",
        "",
    ]

    for table in tables:
        cols = insp.get_columns(table, schema=schema)
        pk_info = insp.get_pk_constraint(table, schema=schema)
        pk_cols = set(pk_info.get("constrained_columns", []))

        # Detect identity columns via raw query
        with engine.connect() as conn:
            identity_result = conn.execute(text(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                  AND TABLE_NAME   = '{table}'
                  AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
            """))
            identity_cols = {row[0] for row in identity_result}

        col_defs = []
        for col in cols:
            is_identity = col["name"] in identity_cols
            if target in ("aurora", "alloydb"):
                col_defs.append(pg_column_def(col, is_identity))
            else:
                col_defs.append(azure_column_def(col, is_identity))

        if pk_cols:
            pk_list = ", ".join(f'"{c}"' for c in pk_info["constrained_columns"])
            col_defs.append(f"  PRIMARY KEY ({pk_list})")

        tbl_name = f'"{table}"' if target in ("aurora", "alloydb") else f"[{table}]"
        lines.append(f"CREATE TABLE IF NOT EXISTS {tbl_name} (")
        lines.append(",\n".join(col_defs))
        lines.append(");")
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"✓ DDL written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="SQL Server → Cloud DDL exporter")
    parser.add_argument("--source",  required=True)
    parser.add_argument("--target",  required=True, choices=["aurora", "azure", "alloydb"])
    parser.add_argument("--schema",  default="dbo")
    parser.add_argument("--tables",  nargs="*", default=[])
    parser.add_argument("--output",  default="schema_output.sql")
    args = parser.parse_args()

    export_schema(
        source_dsn=args.source,
        schema=args.schema,
        target=args.target,
        tables=args.tables,
        output_path=Path(args.output),
    )


if __name__ == "__main__":
    main()
