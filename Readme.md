# SQL Server → Cloud Migration Toolkit

Migrate on-premise SQL Server databases to Google AlloyDB using a modular Python ETL pipeline.

---

## Files

| File               | Purpose                                              |
|--------------------|------------------------------------------------------|
| `migrate.py`       | Main orchestrator — runs the full ETL pipeline       |
| `etl_core.py`      | Extract / Transform / Load engine with retry logic   |
| `cloud_targets.py` | Engine factory for Aurora, Azure SQL, AlloyDB        |
| `schema_export.py` | Export SQL Server DDL → cloud-compatible CREATE TABLE|E.
| `validate.py`      | Post-migration row-count & checksum validation       |
| `requirements.txt` | Python dependencies                                  |

---

## Prerequisites

```bash
pip install -r requirements.txt
```

Install ODBC Driver for SQL Server:
- **Windows**: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- **Linux**: `sudo apt-get install msodbcsql18`
- **macOS**: `brew install microsoft/mssql-release/msodbcsql18`

---

## Quick Start

### 1 — Export the schema

```bash
# Generate PostgreSQL-compatible DDL (for Aurora or AlloyDB)
python schema_export.py \
    --source "mssql+pyodbc://sa:Password1@localhost/Northwind?driver=ODBC+Driver+17+for+SQL+Server" \
    --target aurora \
    --schema dbo \
    --output schema_aurora.sql

# Apply DDL on the target
psql -h your-aurora-cluster -U admin -d yourdb -f schema_aurora.sql
```

### 2 — Migrate data

``` Google AlloyDB (via AlloyDB Auth Proxy on localhost)
python migrate.py \
    --source "mssql+pyodbc://sa:Password1@localhost/Northwind?driver=ODBC+Driver+17+for+SQL+Server" \
    --target alloydb \
    --target-dsn "postgresql+psycopg2://admin:pass@127.0.0.1:5432/northwind" \
    --batch-size 5000
```

### 3 — Validate

```bash
python validate.py \
    --source "mssql+pyodbc://sa:Password1@localhost/Northwind?driver=ODBC+Driver+17+for+SQL+Server" \
    --target-dsn "postgresql+psycopg2://admin:pass@cluster.cluster-xxxx.us-east-1.rds.amazonaws.com:5432/northwind" \
    --schema dbo \
    --checksum
```

---

## Useful Flags

| Flag            | Default | Description                                |
|-----------------|---------|--------------------------------------------|
| `--batch-size`  | 5000    | Rows fetched/written per chunk             |
| `--workers`     | 4       | Parallel table migration threads           |
| `--dry-run`     | off     | Preview without writing to target          |
| `--truncate`    | off     | Truncate target table before each load     |
| `--tables`      | all     | Space-separated list of specific tables    |
| `--schema`      | dbo     | Source SQL Server schema                   |

---

## Type Mapping Reference (SQL Server → PostgreSQL)

| SQL Server        | Aurora / AlloyDB     |
|-------------------|----------------------|
| BIT               | BOOLEAN              |
| TINYINT           | SMALLINT             |
| MONEY             | NUMERIC(19,4)        |
| NVARCHAR(n)       | VARCHAR(n)           |
| NTEXT             | TEXT                 |
| IMAGE             | BYTEA                |
| UNIQUEIDENTIFIER  | UUID                 |
| DATETIME / DATETIME2 | TIMESTAMP         |
| DATETIMEOFFSET    | TIMESTAMPTZ          |
| XML               | TEXT                 |

---

## Tips

- **Large tables**: increase `--batch-size` to 50000 for tables > 10M rows and ensure your cloud target has `work_mem` ≥ 256 MB.
- **AlloyDB**: run the [AlloyDB Auth Proxy](https://cloud.google.com/alloydb/docs/auth-proxy/overview) locally for secure connectivity.
- **Azure SQL**: ensure the firewall rule allows connections from your migration host IP.
- **Identity columns**: `schema_export.py` automatically converts `IDENTITY(1,1)` → `SERIAL`/`BIGSERIAL` on PostgreSQL targets and preserves `IDENTITY` on Azure SQL.
- **Stored procedures / views**: not migrated by this toolkit — use SQL Server Migration Assistant (SSMA) for procedural code conversion.
