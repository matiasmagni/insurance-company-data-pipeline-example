#!/usr/bin/env python3
"""
Load Kaggle to MinIO: Kaggle CSV → MinIO (Raw Layer)
=============================================
Loads Kaggle/downloaded CSV files to MinIO raw layer.

Usage:
    python scripts/load_kaggle_to_minio.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9900"),
    "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    "bucket": os.getenv("MINIO_BUCKET", "insurance-data"),
}

DATA_DIR = Path(__file__).parent.parent / "data"


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
    except Exception:
        s3_client.create_bucket(Bucket=MINIO_CONFIG["bucket"])
        logger.info(f"Created bucket '{MINIO_CONFIG['bucket']}'")


def load_csv_to_minio(s3_client, csv_path: Path, table_name: str):
    """Load a CSV file to MinIO as Parquet."""
    logger.info(f"Loading CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"  Rows: {len(df)}, Columns: {len(df.columns)}")

    if df.empty:
        logger.warning(f"Empty CSV: {csv_path}, skipping")
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


def run_kaggle_load_pipeline():
    """Run the Kaggle to MinIO pipeline."""
    logger.info("=" * 60)
    logger.info("Load Pipeline: Kaggle CSV → MinIO (Raw Layer)")
    logger.info("=" * 60)

    s3_client = None

    try:
        logger.info("Connecting to MinIO...")
        s3_client = get_minio_client()

        logger.info("Ensuring bucket exists...")
        ensure_bucket_exists(s3_client)

        data_dir = Path(__file__).parent.parent / "data"
        if not data_dir.exists():
            logger.warning(f"Data directory not found: {data_dir}")
            logger.info("Run 'python scripts/download_kaggle_data.py' first")
            return 0

        csv_files = [
            ("customer_profiles.csv", "kaggle_customers"),
            ("vehicle_insurance_claims.csv", "kaggle_claims"),
            ("kaggle/vehicle_insurance_claims.csv", "kaggle_claims"),
        ]

        for csv_pattern, table_name in csv_files:
            csv_path = data_dir / csv_pattern
            if csv_path.exists():
                load_csv_to_minio(s3_client, csv_path, table_name)
            else:
                logger.debug(f"CSV not found: {csv_path}")

        logger.info("=" * 60)
        logger.info("Kaggle Load Pipeline completed!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

    finally:
        logger.info("Connections closed")


if __name__ == "__main__":
    run_kaggle_load_pipeline()
