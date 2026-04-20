#!/usr/bin/env python3
"""
DLT Pipeline: PostgreSQL → MinIO (Raw Layer)
===================================
Extracts data from PostgreSQL and loads to MinIO raw/ bucket using DLT.

Usage:
    python scripts/dlt_pipeline.py
"""

import os
import sys
import logging
from pathlib import Path

import dlt
from dlt.sources.sql_database import sql_database

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_dlt_pipeline():
    """Run the DLT pipeline to move data from Postgres to MinIO."""
    logger.info("=" * 60)
    logger.info("DLT Pipeline: PostgreSQL → MinIO raw/")
    logger.info("=" * 60)

    # Database connection string
    pg_user = os.getenv("POSTGRES_USER", "insurance_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "insurance_pass")
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5435")
    pg_db = os.getenv("POSTGRES_DB", "insurance_db")
    
    conn_str = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    # MinIO (S3) destination config
    minio_bucket = os.getenv("MINIO_BUCKET", "insurance-data")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9900")
    minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    # Set environment variables for DLT filesystem destination
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = f"s3://{minio_bucket}"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"] = f"http://{minio_endpoint}"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = minio_user
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = minio_pass

    try:
        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name="postgres_to_minio",
            destination="filesystem",
            dataset_name="raw",
        )

        # Source from Postgres
        # We only want customers and claims tables
        source = sql_database(conn_str).with_resources("customers", "claims")

        # Run pipeline
        # Use parquet format as per README
        load_info = pipeline.run(source, loader_file_format="parquet")

        logger.info(f"DLT Pipeline completed! Load info: {load_info}")

    except Exception as e:
        logger.error(f"DLT Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_dlt_pipeline()
