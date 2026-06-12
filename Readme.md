

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
## This project explores a modular workflow for migrating relational databases to modern cloud platforms.  
The pipeline separates schema extraction, ETL processing, validation, and migration so that different cloud database targets can be supported with minimal changes.



## Supported cloud targets

| Platform | Status | Notes |
|---|---|---|
| Google AlloyDB | Tested and validated | Migrated and checksummed 
| Azure SQL |  Tested and validated | Migrated and checksummed 
| Amazon Aurora |  Tested and validated | Migrated and checksummed 






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

| Target | Tables | Row count | Result |
|---|---|---|---|
| Google AlloyDB | 1 | src=5 tgt=5 | PASS |
| Azure SQL | 13 | src=3,308 tgt=3,308 | PASS |
| Amazon Aurora | 11 | src=3,308 tgt=3,308 | PASS |
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
- Azure account for Azure SQL
- AWS account for Amazon Aurora

---
## Disclaimer

This project is provided for educational, research, and operational reference purposes. It demonstrates one approach to database schema migration, ETL processing, and validation across multiple cloud database platforms.

The scripts and examples are provided "AS IS", without warranty of any kind. Users are responsible for reviewing, testing, and validating all code before use in development, staging, or production environments.

The author makes no guarantees regarding performance, compatibility, security, data integrity, regulatory compliance, or suitability for any specific use case. Always perform backups and testing before executing migrations against live systems.
## License

Copyright (c) 2026 Misty Collins

Licensed under the MIT License.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, subject to the conditions of the MIT License.

See the LICENSE file in this repository for the full license text.



