#!/usr/bin/env python3
"""
  Script:        cloud_targets.py
  Author:        Misty Collins
  Notes:         Engine factory for cloud database targets.

Supported targets:
  • alloydb  — Google AlloyDB (PostgreSQL)
  • aurora   — Amazon Aurora PostgreSQL
  • azure    — Azure SQL (T-SQL via pyodbc)

Usage:
    from cloud_targets import get_target_engine
    engine = get_target_engine("aurora", "postgresql+psycopg2://...")
"""

import logging
import sqlalchemy as sa

log = logging.getLogger(__name__)


def get_target_engine(target: str, dsn: str) -> sa.engine.Engine:
    """
    Build and return a SQLAlchemy engine for the given cloud target.

    Parameters
    ----------
    target : str
        One of: alloydb, aurora, azure
    dsn : str
        SQLAlchemy-compatible connection string for the target.

    Returns
    -------
    sqlalchemy.engine.Engine
    """
    target = target.lower().strip()

    if target == "alloydb":
        engine = sa.create_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        log.info("✓ Target engine created: ALLOYDB (PostgreSQL)")

    elif target == "aurora":
        engine = sa.create_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={
                "connect_timeout": 10,
                "sslmode": "require",
            },
        )
        log.info("✓ Target engine created: AMAZON AURORA (PostgreSQL)")

    elif target == "azure":
        engine = sa.create_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={"timeout": 30},
        )
        log.info("✓ Target engine created: AZURE SQL")

    else:
        raise ValueError(f"Unknown target '{target}'. Choose: alloydb, aurora, azure")

    return engine