"""
============================================================================
Insurance Company Data Pipeline - L1 Unit Tests (Integrated/Mocks)
============================================================================
L1: Unit tests with mocked dependencies (S3, Postgres).
    - No real services required.
    - Uses moto for S3 mocking.
    - Uses pytest-mock for DB mocking.

Usage:
    pytest tests/test_L1_unit_integrated.py -v
============================================================================
"""

import pytest
import os
import boto3
import pandas as pd
from moto import mock_aws
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transformations import apply_risk_buckets
from scripts.silver_transform import transform_customers, MINIO_CONFIG


class TestSilverTransformMocked:
    """L1: Test silver transformation using mocks for S3 and DB."""

    @mock_aws
    def test_transform_customers_flow(self, mocker):
        """Test the full customer transform flow with S3 mocks."""
        # 1. Setup Mock S3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=MINIO_CONFIG["bucket"])
        
        # 2. Upload dummy raw data
        df_raw = pd.DataFrame({
            "customer_id": [1],
            "credit_score": [800],
            "email": ["test@example.com"]
        })
        df_raw.to_parquet("/tmp/test_raw_l1.parquet")
        s3.upload_file("/tmp/test_raw_l1.parquet", MINIO_CONFIG["bucket"], "raw/customers/test.parquet")
        
        # 3. Mock get_minio_client to return our moto client
        mocker.patch("scripts.silver_transform.get_minio_client", return_value=s3)
        
        # 4. Run transform
        transform_customers()
        
        # 5. Verify S3 output
        response = s3.list_objects_v2(Bucket=MINIO_CONFIG["bucket"], Prefix="silver/silver_customers/")
        assert "Contents" in response
        
        # 6. Download and verify content
        silver_key = response["Contents"][0]["Key"]
        s3.download_file(MINIO_CONFIG["bucket"], silver_key, "/tmp/test_silver_l1.parquet")
        df_silver = pd.read_parquet("/tmp/test_silver_l1.parquet")
        
        assert df_silver.iloc[0]["risk_bucket"] == "Excellent"

    def test_dlt_pipeline_structure(self):
        """Verify DLT pipeline script has the correct structure and imports."""
        from scripts.dlt_pipeline import run_dlt_pipeline
        assert callable(run_dlt_pipeline)
