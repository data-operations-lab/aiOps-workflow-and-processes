#!/usr/bin/env python3
"""
  Script:        cloud_targets.py
  Author:        Misty Collins
  Notes:         Engine factory for cloud database targets.
"""

import logging
import sqlalchemy as sa

log = logging.getLogger(__name__)


def get_target_engine(target: str, dsn: str) -> sa.engine.Engine:
    target = target.lower().strip()

    if target in ("alloydb", "aurora"):
        engine = sa.create_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        log.info(f"✓ Target engine created: {target.upper()} (PostgreSQL)")

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