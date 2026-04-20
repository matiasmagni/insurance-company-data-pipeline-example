#!/usr/bin/env python3
"""
Silver Transform: MinIO raw → MinIO silver
=====================================
Reads raw Parquet from MinIO (both PG and Kaggle), 
applies transformations from src/transformations.py, 
unifies the datasets, and writes to silver/.

Usage:
    python scripts/silver_transform.py
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import boto3
import pandas as pd

# Add project root to sys.path to import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transformations import (
    apply_risk_buckets,
    apply_claim_status_categories,
    apply_vehicle_categories,
)

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


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
        aws_access_key_id=MINIO_CONFIG["access_key"],
        aws_secret_access_key=MINIO_CONFIG["secret_key"],
    )


def get_all_objects(s3_client, prefix: str) -> List[str]:
    """Get all object keys in a prefix."""
    response = s3_client.list_objects_v2(
        Bucket=MINIO_CONFIG["bucket"],
        Prefix=prefix,
    )

    if "Contents" not in response or not response["Contents"]:
        return []

    return [obj["Key"] for row in [response["Contents"]] for obj in row]


def read_parquet_from_s3(s3_client, key: str) -> pd.DataFrame:
    """Download and read a parquet file from S3."""
    local_path = f"/tmp/{os.path.basename(key)}"
    s3_client.download_file(MINIO_CONFIG["bucket"], key, local_path)
    df = pd.read_parquet(local_path)
    os.unlink(local_path)
    return df


def transform_customers():
    """Unify and transform customers from PG and Kaggle."""
    s3 = get_minio_client()
    
    # 1. Load PG customers (from DLT 'raw' dataset)
    pg_keys = get_all_objects(s3, "raw/customers/")
    pg_dfs = [read_parquet_from_s3(s3, k) for k in pg_keys]
    pg_df = pd.concat(pg_dfs) if pg_dfs else pd.DataFrame()
    if not pg_df.empty:
        pg_df["source_system"] = "postgres"
        pg_df["customer_id"] = pg_df["customer_id"].astype(str)
    
    # 2. Load Kaggle customers
    kg_keys = get_all_objects(s3, "raw/kaggle_customers/")
    kg_dfs = [read_parquet_from_s3(s3, k) for k in kg_keys]
    kg_df = pd.concat(kg_dfs) if kg_dfs else pd.DataFrame()
    if not kg_df.empty:
        kg_df["source_system"] = "kaggle"
        kg_df["customer_id"] = kg_df["customer_id"].astype(str)
        # Normalize Kaggle columns to match PG if possible
        if "name" in kg_df.columns:
            # Split name into first and last
            kg_df["first_name"] = kg_df["name"].str.split().str[0]
            kg_df["last_name"] = kg_df["name"].str.split().str[1:].str.join(" ")
    
    # 3. Unified DataFrame
    unified_df = pd.concat([pg_df, kg_df], ignore_index=True)
    if unified_df.empty:
        logger.warning("No customer data found to transform")
        return

    # Remove DLT metadata columns if they exist
    dlt_cols = [c for c in unified_df.columns if c.startswith("_dlt")]
    if dlt_cols:
        unified_df = unified_df.drop(columns=dlt_cols)

    # 4. Apply transformations from src/
    unified_df = apply_risk_buckets(unified_df)
    
    # 5. Save to Silver
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_key = f"silver/silver_customers/customers_{timestamp}.parquet"
    local_path = "/tmp/silver_customers.parquet"
    unified_df.to_parquet(local_path, index=False)
    s3.upload_file(local_path, MINIO_CONFIG["bucket"], silver_key)
    os.unlink(local_path)
    
    logger.info(f"Transformed {len(unified_df)} customers to {silver_key}")


def transform_claims():
    """Unify and transform claims from PG and Kaggle."""
    s3 = get_minio_client()
    
    # 1. Load PG claims
    pg_keys = get_all_objects(s3, "raw/claims/")
    pg_dfs = [read_parquet_from_s3(s3, k) for k in pg_keys]
    pg_df = pd.concat(pg_dfs) if pg_dfs else pd.DataFrame()
    if not pg_df.empty:
        pg_df["source_system"] = "postgres"
        pg_df["customer_id"] = pg_df["customer_id"].astype(str)
        pg_df["claim_id"] = pg_df["claim_id"].astype(str)
        if "claim_date" in pg_df.columns:
            pg_df["claim_date"] = pd.to_datetime(pg_df["claim_date"])
        if "claim_amount" in pg_df.columns:
            pg_df["claim_amount"] = pg_df["claim_amount"].astype(float)
        if "claim_paid_amount" in pg_df.columns:
            pg_df["claim_paid_amount"] = pg_df["claim_paid_amount"].astype(float)
    
    # 2. Load Kaggle claims
    kg_keys = get_all_objects(s3, "raw/kaggle_claims/")
    kg_dfs = [read_parquet_from_s3(s3, k) for k in kg_keys]
    kg_df = pd.concat(kg_dfs) if kg_dfs else pd.DataFrame()
    if not kg_df.empty:
        kg_df["source_system"] = "kaggle"
        kg_df["claim_id"] = kg_df["claim_id"].astype(str)
        if "claim_date" in kg_df.columns:
            kg_df["claim_date"] = pd.to_datetime(kg_df["claim_date"])
        if "claim_amount" in kg_df.columns:
            kg_df["claim_amount"] = kg_df["claim_amount"].astype(float)
        if "claim_paid_amount" in kg_df.columns:
            kg_df["claim_paid_amount"] = kg_df["claim_paid_amount"].astype(float)
        if "policy_number" in kg_df.columns and "customer_id" not in kg_df.columns:
            kg_df["customer_id"] = kg_df["policy_number"].astype(str)
        elif "customer_id" in kg_df.columns:
            kg_df["customer_id"] = kg_df["customer_id"].astype(str)
    
    # 3. Unified DataFrame
    unified_df = pd.concat([pg_df, kg_df], ignore_index=True)
    if unified_df.empty:
        logger.warning("No claims data found to transform")
        return

    # Remove DLT metadata columns if they exist
    dlt_cols = [c for c in unified_df.columns if c.startswith("_dlt")]
    if dlt_cols:
        unified_df = unified_df.drop(columns=dlt_cols)

    # 4. Apply transformations from src/
    unified_df = apply_claim_status_categories(unified_df)
    unified_df = apply_vehicle_categories(unified_df)
    
    # 5. Save to Silver
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_key = f"silver/silver_claims/claims_{timestamp}.parquet"
    local_path = "/tmp/silver_claims.parquet"
    unified_df.to_parquet(local_path, index=False)
    s3.upload_file(local_path, MINIO_CONFIG["bucket"], silver_key)
    os.unlink(local_path)
    
    logger.info(f"Transformed {len(unified_df)} claims to {silver_key}")


def run_silver_transform():
    logger.info("=" * 60)
    logger.info("Silver Transform: MinIO raw/ → MinIO silver/")
    logger.info("=" * 60)

    try:
        transform_customers()
        transform_claims()
        logger.info("Silver Transform completed successfully!")
    except Exception as e:
        logger.error(f"Silver Transform failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_silver_transform()
