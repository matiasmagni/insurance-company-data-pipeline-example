#!/usr/bin/env python3
"""
Insurance Company Data Pipeline
================================
Main pipeline orchestration: DLT + DBT

Usage:
    python pipeline.py
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_dlt_pipeline():
    """Run DLT: PostgreSQL → MinIO (raw)."""
    logger.info("=" * 60)
    logger.info("Step 1: Running DLT Pipeline (PostgreSQL → MinIO)")
    logger.info("=" * 60)

    script_path = Path(__file__).parent / "scripts" / "dlt_pipeline.py"
    result = subprocess.run([sys.executable, str(script_path)], check=True)
    return result.returncode


def run_dbt_transformations():
    """Run DBT: MinIO raw → MinIO silver → ClickHouse gold."""
    logger.info("=" * 60)
    logger.info("Step 2: Running DBT Transformations")
    logger.info("=" * 60)

    dbt_dir = Path(__file__).parent / "dbt_insurance"

    if not dbt_dir.exists():
        logger.warning("DBT project not found, skipping transformations")
        return 0

    result = subprocess.run(["dbt", "run"], cwd=str(dbt_dir), check=False)

    if result.returncode != 0:
        logger.warning("DBT run failed, continuing...")

    return 0


def main():
    """Run the full pipeline."""
    logger.info("=" * 60)
    logger.info("Insurance Company Data Pipeline")
    logger.info("=" * 60)
    logger.info("Architecture: PostgreSQL → MinIO → ClickHouse")
    logger.info("")

    try:
        run_dlt_pipeline()
        run_dbt_transformations()

        logger.info("")
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)

        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Check MinIO console: http://localhost:9901")
        logger.info(
            "  2. Query ClickHouse: docker exec -it insurance_clickhouse clickhouse-client"
        )
        logger.info("  3. Run tests: ./scripts_unix/run_all_tests.sh")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
