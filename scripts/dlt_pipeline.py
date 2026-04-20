#!/usr/bin/env python3
"""
DLT Pipeline: PostgreSQL → MinIO (Raw Layer)
====================================
Extracts data from PostgreSQL and loads to MinIO as Parquet files.

Usage:
    python scripts/dlt_pipeline.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "insurance_db"),
    "user": os.getenv("POSTGRES_USER", "insurance_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "insurance_pass"),
}

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9900"),
    "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    "bucket": os.getenv("MINIO_BUCKET", "insurance-data"),
}

RAW_TABLES = ["customers", "claims"]


def get_postgres_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)


def get_minio_client():
    """Create MinIO/S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
        aws_access_key_id=MINIO_CONFIG["access_key"],
        aws_secret_access_key=MINIO_CONFIG["secret_key"],
    )


def ensure_bucket_exists(s3_client):
    """Ensure MinIO bucket exists."""
    try:
        s3_client.head_bucket(Bucket=MINIO_CONFIG["bucket"])
        logger.info(f"Bucket '{MINIO_CONFIG['bucket']}' already exists")
    except Exception:
        s3_client.create_bucket(Bucket=MINIO_CONFIG["bucket"])
        logger.info(f"Created bucket '{MINIO_CONFIG['bucket']}'")


def export_table_to_parquet(s3_client, table_name: str, conn):
    """Export a PostgreSQL table to MinIO as Parquet."""
    query = f"SELECT * FROM {table_name}"

    logger.info(f"Exporting table: {table_name}")
    df = pd.read_sql(query, conn)

    if df.empty:
        logger.warning(f"Table {table_name} is empty, skipping")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{table_name}_{timestamp}.parquet"

    local_path = Path("/tmp") / filename
    df.to_parquet(local_path, index=False)

    s3_key = f"raw/{table_name}/{filename}"
    s3_client.upload_file(
        str(local_path),
        MINIO_CONFIG["bucket"],
        s3_key,
    )

    local_path.unlink()

    logger.info(f"Loaded {len(df)} rows to s3://{MINIO_CONFIG['bucket']}/{s3_key}")


def run_dlt_pipeline():
    """Run the DLT pipeline."""
    logger.info("=" * 60)
    logger.info("DLT Pipeline: PostgreSQL → MinIO (Raw Layer)")
    logger.info("=" * 60)

    conn = None
    s3_client = None

    try:
        logger.info("Connecting to PostgreSQL...")
        conn = get_postgres_connection()

        logger.info("Connecting to MinIO...")
        s3_client = get_minio_client()

        logger.info("Ensuring bucket exists...")
        ensure_bucket_exists(s3_client)

        for table in RAW_TABLES:
            export_table_to_parquet(s3_client, table, conn)

        logger.info("=" * 60)
        logger.info("DLT Pipeline completed successfully!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
        logger.info("Connections closed")


if __name__ == "__main__":
    run_dlt_pipeline()
