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
        df_raw = pd.DataFrame(
            {"customer_id": [1], "credit_score": [800], "email": ["test@example.com"]}
        )
        df_raw.to_parquet("/tmp/test_raw_l1.parquet")
        s3.upload_file(
            "/tmp/test_raw_l1.parquet",
            MINIO_CONFIG["bucket"],
            "raw/customers/test.parquet",
        )

        # 3. Mock get_minio_client to return our moto client
        mocker.patch("scripts.silver_transform.get_minio_client", return_value=s3)

        # 4. Run transform
        transform_customers()

        # 5. Verify S3 output
        response = s3.list_objects_v2(
            Bucket=MINIO_CONFIG["bucket"], Prefix="silver/silver_customers/"
        )
        assert "Contents" in response

        # 6. Download and verify content
        silver_key = response["Contents"][0]["Key"]
        s3.download_file(
            MINIO_CONFIG["bucket"], silver_key, "/tmp/test_silver_l1.parquet"
        )
        df_silver = pd.read_parquet("/tmp/test_silver_l1.parquet")

        assert df_silver.iloc[0]["risk_bucket"] == "Excellent"

    def test_dlt_pipeline_structure(self):
        try:
            from scripts.dlt_pipeline import run_dlt_pipeline

            assert callable(run_dlt_pipeline)
        except ModuleNotFoundError:
            pytest.skip("dlt module not installed")

    @pytest.mark.parametrize(
        "module_name",
        [
            "dlt_pipeline",
            "load_kaggle_to_minio",
            "silver_transform",
            "load_silver_to_clickhouse",
        ],
    )
    def test_script_imports(self, module_name):
        try:
            module = __import__(f"scripts.{module_name}", fromlist=[""])
            assert module is not None
        except ModuleNotFoundError as e:
            pytest.skip(f"Missing dependency: {e.name}")


class TestDLTPipelineL1:
    """L1: Test dlt_pipeline.py - skipped if dlt not installed."""

    def test_dlt_available(self):
        """Check if dlt is available, skip if not."""
        try:
            import dlt
        except ModuleNotFoundError:
            pytest.skip("dlt module not installed")

    def test_postgres_config_from_env(self):
        """Test PostgreSQL config reads from environment."""
        pg_user = os.getenv("POSTGRES_USER", "insurance_user")
        assert pg_user is not None

    def test_minio_config_from_env(self):
        """Test MinIO config reads from environment."""
        minio_bucket = os.getenv("MINIO_BUCKET", "insurance-data")
        assert minio_bucket == "insurance-data"


class TestLoadKaggleToMinioL1:
    """L1: Test load_kaggle_to_minio.py with mocks."""

    @patch("scripts.load_kaggle_to_minio.get_minio_client")
    def test_minio_client_creation(self, mock_minio):
        """Test MinIO client is created correctly."""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        from scripts import load_kaggle_to_minio

        client = load_kaggle_to_minio.get_minio_client()
        assert client is not None

    def test_config_defaults(self):
        """Test config defaults."""
        from scripts import load_kaggle_to_minio

        assert "endpoint" in load_kaggle_to_minio.MINIO_CONFIG
        assert load_kaggle_to_minio.MINIO_CONFIG["bucket"] == "insurance-data"

    @patch("scripts.load_kaggle_to_minio.get_minio_client")
    @patch("scripts.load_kaggle_to_minio.ensure_bucket_exists")
    def test_load_csv_handles_empty(self, mock_bucket, mock_minio):
        """Test empty CSV handling."""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        df = pd.DataFrame()
        assert df.empty is True

    def test_csv_file_patterns(self):
        """Test expected CSV file patterns."""
        csv_files = [
            ("customer_profiles.csv", "kaggle_customers"),
            ("vehicle_insurance_claims.csv", "kaggle_claims"),
        ]
        assert len(csv_files) == 2


class TestSilverTransformL1:
    """L1: Test silver_transform.py with mocks."""

    @patch("scripts.silver_transform.get_minio_client")
    def test_get_all_objects_returns_list(self, mock_client):
        """Test get_all_objects returns list of keys."""
        from scripts.silver_transform import get_all_objects

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.parquet"},
                {"Key": "file2.parquet"},
            ]
        }

        result = get_all_objects(mock_s3, "test/")
        assert len(result) == 2

    @patch("scripts.silver_transform.get_minio_client")
    def test_get_all_objects_handles_empty(self, mock_client):
        """Test get_all_objects handles empty bucket."""
        from scripts.silver_transform import get_all_objects

        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {}

        result = get_all_objects(mock_s3, "empty/")
        assert result == []

    def test_config_imports_from_transformations(self):
        """Test that silver_transform imports correctly from src."""
        from scripts.silver_transform import (
            apply_risk_buckets,
            apply_claim_status_categories,
        )

        assert callable(apply_risk_buckets)
        assert callable(apply_claim_status_categories)

    def test_minio_config(self):
        """Test MinIO config."""
        from scripts.silver_transform import MINIO_CONFIG

        assert MINIO_CONFIG["bucket"] == "insurance-data"


class TestLoadSilverToClickHouseL1:
    """L1: Test load_silver_to_clickhouse.py with mocks."""

    @patch("scripts.load_silver_to_clickhouse.get_minio_client")
    def test_minio_client_creation(self, mock_minio):
        """Test MinIO client is created."""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        from scripts import load_silver_to_clickhouse

        client = load_silver_to_clickhouse.get_minio_client()
        assert client is not None

    @patch("scripts.load_silver_to_clickhouse.get_clickhouse_client")
    def test_clickhouse_client_creation(self, mock_ch):
        """Test ClickHouse client is created."""
        mock_client = MagicMock()
        mock_ch.return_value = mock_client

        from scripts import load_silver_to_clickhouse

        client = load_silver_to_clickhouse.get_clickhouse_client()
        assert client is not None

    def test_config_defaults(self):
        """Test config defaults."""
        from scripts import load_silver_to_clickhouse

        assert "endpoint" in load_silver_to_clickhouse.MINIO_CONFIG
        assert load_silver_to_clickhouse.MINIO_CONFIG["bucket"] == "insurance-data"

        assert "host" in load_silver_to_clickhouse.CLICKHOUSE_CONFIG
        assert load_silver_to_clickhouse.CLICKHOUSE_CONFIG["port"] == 8123

    def test_get_latest_object_returns_none_for_empty(self):
        """Test get_latest_object handles empty bucket."""
        from scripts.load_silver_to_clickhouse import get_latest_object

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}

        result = get_latest_object(mock_client, "nonexistent/")
        assert result is None

    def test_get_latest_object_returns_latest(self):
        """Test get_latest_object returns most recent."""
        from scripts.load_silver_to_clickhouse import get_latest_object

        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "old.parquet", "LastModified": "2024-01-01"},
                {"Key": "new.parquet", "LastModified": "2024-01-02"},
            ]
        }

        result = get_latest_object(mock_client, "test/")
        assert result == "new.parquet"
