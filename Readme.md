

## Cross-Cloud Database Migration Workflow

```mermaid
flowchart LR
    A[Source Database<br>SQL Server or PostgreSQL]
    B[Schema Export]
    C[ETL Processing]
    D[Validation]
    E[Migration Engine]

    F[GCP Targets<br>AlloyDB / Cloud SQL]
    G[Azure Targets<br>Azure SQL / Postgres]
    H[AWS Targets<br>RDS / Aurora]

    A --> B
    B --> C
    C --> D
    D --> E

    E --> F
    E --> G
    E --> H
```

## What this does

Migrates on-premise SQL Server databases to cloud-native platforms 
using a modular Python ETL pipeline. Built from scratch as a 
self-teaching project by a SQL Server DBA returning to the field.

---

## Supported cloud targets

| Platform | Status | Notes |
|---|---|---|
| Google AlloyDB | Tested and validated | Migrated and checksummed |
| Azure SQL | Coming soon | In progress |
| Amazon Aurora | Coming soon | In progress |

---
## Cross-Cloud Database Migration Workflow

```mermaid
flowchart LR
    A[Source Database<br>SQL Server or PostgreSQL]
    B[Schema Export]
    C[ETL Processing]
    D[Validation]
    E[Migration Engine]

    F[GCP Targets<br>AlloyDB / Cloud SQL]
    G[Azure Targets<br>Azure SQL / Postgres]
    H[AWS Targets<br>RDS / Aurora]

    A --> B
    B --> C
    C --> D
    D --> E

    E --> F
    E --> G
    E --> H


## Scripts

| File | Purpose |
|---|---|
| `migrate.py` | Main ETL orchestrator — runs the full pipeline |
| `etl_core.py` | Extract, transform, load engine with retry logic |
| `cloud_targets.py` | Cloud connection factory for all three targets |
| `schema_export.py` | DDL export and MSSQL to PostgreSQL type conversion |
| `validate.py` | Post-migration row count and checksum validation |

---

## Quick start
```bash
# Install dependencies
pip install -r requirements.txt

# Export schema
python schema_export.py \
  --source "mssql+pyodbc://user:pass@localhost/dbname?driver=ODBC+Driver+17+for+SQL+Server" \
  --target alloydb \
  --output schema.sql

# Migrate data
python migrate.py \
  --source "mssql+pyodbc://user:pass@localhost/dbname?driver=ODBC+Driver+17+for+SQL+Server" \
  --target alloydb \
  --target-dsn "postgresql+psycopg2://user:pass@127.0.0.1:5432/postgres" \
  --batch-size 5000

# Validate
python validate.py \
  --source "mssql+pyodbc://user:pass@localhost/dbname?driver=ODBC+Driver+17+for+SQL+Server" \
  --target-dsn "postgresql+psycopg2://user:pass@127.0.0.1:5432/postgres" \
  --checksum
```



## Validated results

| Check | Result |
|---|---|
| Row count | PASS — src=5 tgt=5 |
| Checksum | PASS — src=15.0 tgt=15.0 |
| Tables failed | 0 |

---

## Tech stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.14 | Runtime |
| SQLAlchemy | 2.0.48 | Database abstraction |
| pandas | 3.0.1 | Data transformation |
| psycopg2 | 2.9.11 | PostgreSQL driver |
| pyodbc | 5.3.0 | SQL Server driver |

---

## Requirements

- Python 3.10+
- ODBC Driver 17 or 18 for SQL Server
- Google Cloud account for AlloyDB
- AlloyDB Auth Proxy for secure tunnel

---

*Built with Python on Pi Day 2026 — SQL Server DBA learning cloud migration from scratch*
