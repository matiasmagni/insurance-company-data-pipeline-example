#!/usr/bin/env python3
"""
Load Silver to ClickHouse: MinIO silver → ClickHouse
=============================================
Loads silver Parquet files from MinIO to ClickHouse tables.
Step 2.5 in the pipeline.

Usage:
    python scripts/load_silver_to_clickhouse.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import clickhouse_connect

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

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", 8123)),
    "database": os.getenv("CLICKHOUSE_DB", "insurance_db"),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass"),
}


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
        aws_access_key_id=MINIO_CONFIG["access_key"],
        aws_secret_access_key=MINIO_CONFIG["secret_key"],
    )


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_CONFIG["host"],
        port=CLICKHOUSE_CONFIG["port"],
        database=CLICKHOUSE_CONFIG["database"],
        username=CLICKHOUSE_CONFIG["user"],
        password=CLICKHOUSE_CONFIG["password"],
    )


def get_latest_object(s3_client, prefix):
    response = s3_client.list_objects_v2(
        Bucket=MINIO_CONFIG["bucket"],
        Prefix=prefix,
    )
    if "Contents" not in response or not response["Contents"]:
        return None
    return sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)[
        0
    ]["Key"]


def load_table(s3_client, ch_client, table_name):
    prefix = f"silver/{table_name}/"
    key = get_latest_object(s3_client, prefix)
    if not key:
        logger.warning(f"No silver file found for {table_name}")
        return

    logger.info(f"Loading {key} into ClickHouse table {table_name}")

    local_path = f"/tmp/{table_name}.parquet"
    s3_client.download_file(MINIO_CONFIG["bucket"], key, local_path)
    df = pd.read_parquet(local_path)
    os.unlink(local_path)

    # Handle NaN values for ClickHouse (replace with None/NULL)
    df = df.where(pd.notnull(df), None)

    # Ensure table exists
    if table_name == "silver_customers":
        ch_client.command("DROP TABLE IF EXISTS silver_customers")
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS silver_customers (
                customer_id String,
                first_name String,
                last_name String,
                email String,
                phone_number Nullable(String),
                date_of_birth Nullable(Date),
                address Nullable(String),
                credit_score Nullable(Int32),
                annual_income Nullable(Float64),
                occupation Nullable(String),
                city Nullable(String),
                state Nullable(String),
                zip_code Nullable(String),
                country Nullable(String),
                risk_bucket String,
                source_system String,
                telematics_score Nullable(Int32),
                policy_number Nullable(String),
                name Nullable(String),
                created_at Nullable(DateTime),
                updated_at Nullable(DateTime)
            ) ENGINE = MergeTree() ORDER BY customer_id
        """)
    elif table_name == "silver_claims":
        ch_client.command("DROP TABLE IF EXISTS silver_claims")
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS silver_claims (
                claim_id String,
                customer_id String,
                claim_date Date,
                claim_type String,
                claim_status String,
                claim_amount Float64,
                claim_paid_amount Nullable(Float64),
                vehicle_type Nullable(String),
                agent_id Nullable(Int32),
                agent_name Nullable(String),
                claim_status_category String,
                vehicle_category String,
                source_system String,
                policy_number Nullable(String),
                driver_age Nullable(Int32),
                fraud_indicator Nullable(String),
                city Nullable(String),
                created_at Nullable(DateTime),
                updated_at Nullable(DateTime)
            ) ENGINE = MergeTree() ORDER BY claim_id
        """)

    # In a real pipeline we might want to truncate first or use a staging table
    try:
        ch_client.command(f"TRUNCATE TABLE {table_name}")
        ch_client.insert_df(table_name, df)
        logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        raise


def run():
    logger.info("=" * 60)
    logger.info("Step 2.5: Load Silver → ClickHouse")
    logger.info("=" * 60)

    try:
        s3 = get_minio_client()
        ch = get_clickhouse_client()

        # Load both silver tables
        load_table(s3, ch, "silver_customers")
        load_table(s3, ch, "silver_claims")

        logger.info("=" * 60)
        logger.info("Loaded Silver to ClickHouse successfully!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"Load failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
