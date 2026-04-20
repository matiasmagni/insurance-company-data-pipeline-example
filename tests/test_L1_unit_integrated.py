"""
================================================================================
Insurance Company Data Pipeline - L1 Unit Tests (Integrated)
================================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)
Eng. Matías J. Magni | CEO @ BugMentor

L1: Unit tests - Integration with mocked dependencies

Usage:
    pytest tests/test_L1_unit_integrated.py -v
================================================================================
"""

import os
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd


@pytest.fixture
def sample_claims_data():
    """Sample claims data from Kaggle CSV."""
    return pd.DataFrame(
        {
            "policy_number": [
                "POL-2024-00001",
                "POL-2024-00002",
                "POL-2024-00003",
            ],
            "claim_amount": [5000.0, 7500.0, 3000.0],
            "claim_date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "fraud_indicator": ["N", "Y", "N"],
            "vehicle_type": ["Sedan", "SUV", "Truck"],
            "driver_age": [35, 28, 45],
        }
    )


@pytest.fixture
def sample_customer_data():
    """Sample customer data from PostgreSQL."""
    return pd.DataFrame(
        {
            "customer_id": ["CUST-00001", "CUST-00002", "CUST-00003"],
            "name": ["John Doe", "Jane Smith", "Bob Johnson"],
            "email": [
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
            ],
            "credit_score": [720, 680, 750],
            "telematics_score": [85.5, 72.3, 91.0],
            "policy_number": [
                "POL-2024-00001",
                "POL-2024-00002",
                "POL-2024-00003",
            ],
        }
    )


class TestCSVExtraction:
    """L1: Tests for CSV data extraction with mocked dependencies."""

    @pytest.mark.L1
    @patch("src.extract_and_load.pd.read_csv")
    @patch("src.extract_and_load.os.path.exists")
    def test_extract_csv_file_exists(
        self, mock_exists, mock_read_csv, sample_claims_data
    ):
        """L1: Test that CSV file existence is checked."""
        from src import extract_and_load

        mock_exists.return_value = True
        mock_read_csv.return_value = sample_claims_data

        result = extract_and_load.extract_csv("/data/claims.csv")

        mock_exists.assert_called_once_with("/data/claims.csv")

    @pytest.mark.L1
    @patch("src.extract_and_load.pd.read_csv")
    @patch("src.extract_and_load.os.path.exists")
    def test_extract_csv_returns_dataframe(
        self, mock_exists, mock_read_csv, sample_claims_data
    ):
        """L1: Test that CSV extraction returns a pandas DataFrame."""
        from src import extract_and_load

        mock_exists.return_value = True
        mock_read_csv.return_value = sample_claims_data

        result = extract_and_load.extract_csv("/data/claims.csv")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "policy_number" in result.columns

    @pytest.mark.L1
    @patch("src.extract_and_load.os.path.exists")
    def test_extract_csv_file_not_found(self, mock_exists):
        """L1: Test that missing CSV file raises FileNotFoundError."""
        from src import extract_and_load

        mock_exists.return_value = False

        with pytest.raises(FileNotFoundError):
            extract_and_load.extract_csv("/data/missing.csv")

    @pytest.mark.L1
    @patch("src.extract_and_load.pd.read_csv")
    @patch("src.extract_and_load.os.path.exists")
    def test_extract_csv_handles_empty_file(self, mock_exists, mock_read_csv):
        """L1: Test that empty CSV file is handled gracefully."""
        from src import extract_and_load

        mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame()

        result = extract_and_load.extract_csv("/data/claims.csv")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestPostgreSQLExtraction:
    """L1: Tests for PostgreSQL data extraction with mocked dependencies."""

    @pytest.mark.L1
    @patch("src.extract_and_load.create_engine")
    @patch("src.extract_and_load.pd.read_sql")
    def test_extract_postgres_returns_dataframe(
        self, mock_read_sql, mock_engine, sample_customer_data
    ):
        """L1: Test that PostgreSQL extraction returns a DataFrame."""
        from src import extract_and_load

        mock_engine.return_value = MagicMock()
        mock_read_sql.return_value = sample_customer_data

        result = extract_and_load.extract_postgres("SELECT * FROM customer_profiles")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "customer_id" in result.columns

    @pytest.mark.L1
    @patch("src.extract_and_load.create_engine")
    @patch("src.extract_and_load.pd.read_sql")
    def test_extract_postgres_uses_correct_query(
        self, mock_read_sql, mock_engine, sample_customer_data
    ):
        """L1: Test that correct SQL query is executed."""
        from src import extract_and_load

        mock_engine.return_value = MagicMock()
        mock_read_sql.return_value = sample_customer_data

        query = "SELECT customer_id, telematics_score FROM customer_profiles WHERE credit_score > 700"
        result = extract_and_load.extract_postgres(query)

        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        assert query in str(call_args)

    @pytest.mark.L1
    @patch("src.extract_and_load.create_engine")
    def test_extract_postgres_connection_error(self, mock_engine):
        """L1: Test that connection errors are handled."""
        from src import extract_and_load

        mock_engine.side_effect = Exception("Connection failed")

        with pytest.raises(Exception):
            extract_and_load.extract_postgres("SELECT * FROM customer_profiles")


class TestMinIOOperations:
    """L1: Tests for MinIO operations with mocked dependencies."""

    @pytest.mark.L1
    @patch("src.extract_and_load.boto3.client")
    def test_create_minio_client_with_http_prefix(self, mock_boto3):
        """L1: Test create_minio_client handles http:// prefix."""
        from src import extract_and_load

        os.environ["MINIO_ENDPOINT"] = "http://localhost:9000"
        os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
        os.environ["MINIO_SECRET_KEY"] = "minioadmin"
        try:
            extract_and_load.create_minio_client()
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs["endpoint_url"] == "http://localhost:9000"
        finally:
            del os.environ["MINIO_ENDPOINT"]

    @pytest.mark.L1
    @patch("google.cloud.bigquery.Client")
    def test_create_bigquery_client(self, mock_bq_client):
        """L1: Test create_bigquery_client creates client."""
        from src import extract_and_load

        extract_and_load.create_bigquery_client()
        mock_bq_client.assert_called_once()

    @pytest.mark.L1
    @patch("src.extract_and_load.boto3.client")
    def test_create_minio_client_with_https_prefix(self, mock_boto3):
        """L1: Test create_minio_client handles https:// prefix."""
        from src import extract_and_load

        os.environ["MINIO_ENDPOINT"] = "https://localhost:9000"
        os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
        os.environ["MINIO_SECRET_KEY"] = "minioadmin"
        try:
            extract_and_load.create_minio_client()
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs["endpoint_url"] == "http://localhost:9000"
        finally:
            del os.environ["MINIO_ENDPOINT"]

    @pytest.mark.L1
    @patch("src.extract_and_load.boto3.client")
    def test_ensure_bucket_exists_success(self, mock_boto3):
        """L1: Test ensure_bucket_exists when bucket exists."""
        from src import extract_and_load

        mock_client = MagicMock()
        mock_boto3.return_value = mock_client
        mock_client.head_bucket.return_value = True
        extract_and_load.ensure_bucket_exists(mock_client, "existing-bucket")
        mock_client.head_bucket.assert_called_once_with(Bucket="existing-bucket")


class TestELTPipeline:
    """L1: Tests for ELT pipeline with mocked dependencies."""

    @pytest.mark.L1
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    def test_run_elt_pipeline_success(
        self, mock_bq, mock_upload, mock_parquet, mock_csv, mock_postgres
    ):
        """L1: Test run_elt_pipeline with all steps mocked correctly."""
        from src import extract_and_load

        os.environ["MINIO_BUCKET"] = "test-bucket"
        os.environ["BIGQUERY_PROJECT_ID"] = "test-project"
        os.environ["BIGQUERY_DATASET"] = "test_dataset"

        claims_df = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        customers_df = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )

        mock_csv.return_value = claims_df
        mock_postgres.return_value = customers_df
        mock_parquet.return_value = b"parquet"

        try:
            result = extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv", postgres_query="SELECT * FROM customers"
            )
            assert result["csv_extract"] is True
            assert result["postgres_extract"] is True
            assert result["claims_to_parquet"] is True
            assert result["customers_to_parquet"] is True
        finally:
            del os.environ["MINIO_BUCKET"]


class TestConfiguration:
    """L1: Tests for configuration with mocked environment."""

    @pytest.mark.L1
    @patch.dict(
        os.environ,
        {
            "POSTGRES_HOST": "test-host",
            "POSTGRES_PORT": "5433",
            "POSTGRES_USER": "test-user",
            "POSTGRES_PASSWORD": "test-pass",
            "POSTGRES_DB": "test-db",
        },
    )
    def test_get_postgres_config_from_env(self):
        """L1: Test PostgreSQL configuration is read from environment."""
        from src import extract_and_load

        config = extract_and_load.get_postgres_config()

        assert config["host"] == "test-host"
        assert config["port"] == "5433"
        assert config["user"] == "test-user"
        assert config["password"] == "test-pass"
        assert config["database"] == "test-db"


class TestErrorHandling:
    """L1: Tests for error handling paths to increase coverage."""

    @pytest.mark.L1
    @patch("src.extract_and_load.boto3.client")
    def test_ensure_bucket_exists_creates_bucket(self, mock_boto3):
        """L1: Test ensure_bucket_exists creates bucket on 404 error."""
        from src import extract_and_load
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_boto3.return_value = mock_client

        error = ClientError(
            {"Error": {"Code": "404", "Message": "NoSuchBucket"}}, "HeadBucket"
        )
        mock_client.head_bucket.side_effect = error

        extract_and_load.ensure_bucket_exists(mock_client, "new-bucket")
        mock_client.create_bucket.assert_called_once_with(Bucket="new-bucket")

    @pytest.mark.L1
    @patch("src.extract_and_load.boto3.client")
    def test_ensure_bucket_exists_raises_on_other_error(self, mock_boto3):
        """L1: Test ensure_bucket_exists raises on non-404 errors."""
        from src import extract_and_load
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_boto3.return_value = mock_client

        error = ClientError(
            {"Error": {"Code": "403", "Message": "AccessDenied"}}, "HeadBucket"
        )
        mock_client.head_bucket.side_effect = error

        with pytest.raises(ClientError):
            extract_and_load.ensure_bucket_exists(mock_client, "test-bucket")

    @pytest.mark.L1
    @patch("src.extract_and_load.create_bigquery_client")
    def test_load_to_bigquery_invalid_table_reference(self, mock_bq_client):
        """L1: Test load_to_bigquery raises on invalid table reference."""
        from src import extract_and_load

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        with pytest.raises(ValueError, match="Invalid table reference"):
            extract_and_load.load_to_bigquery(
                pd.DataFrame({"col": [1]}), "invalid-reference"
            )

    @pytest.mark.L1
    @patch("src.extract_and_load.create_bigquery_client")
    def test_load_to_bigquery_too_many_parts(self, mock_bq_client):
        """L1: Test load_to_bigquery raises on too many parts."""
        from src import extract_and_load

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        with pytest.raises(ValueError, match="Invalid table reference"):
            extract_and_load.load_to_bigquery(
                pd.DataFrame({"col": [1]}), "project.dataset.table.extra"
            )

    @pytest.mark.L1
    @patch("src.extract_and_load.create_bigquery_client")
    def test_load_to_bigquery_success(self, mock_bq_client):
        """L1: Test load_to_bigquery success path."""
        from src import extract_and_load

        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_client.load_table_from_dataframe.return_value = mock_job
        mock_bq_client.return_value = mock_client

        df = pd.DataFrame({"col": [1, 2, 3]})
        result = extract_and_load.load_to_bigquery(df, "project.dataset.table")

        assert result is True
        mock_client.load_table_from_dataframe.assert_called_once()

    @pytest.mark.L1
    def test_dataframe_to_parquet_stream(self):
        """L1: Test dataframe_to_parquet_stream function."""
        from src import extract_and_load
        import io

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = extract_and_load.dataframe_to_parquet_stream(df)

        assert isinstance(result, io.BytesIO)
        result.seek(0)
        read_df = pd.read_parquet(result)
        assert len(read_df) == 3

    @pytest.mark.L1
    def test_generate_parquet_key_with_custom_timestamp(self):
        """L1: Test generate_parquet_key with custom timestamp."""
        from src import extract_and_load
        from datetime import datetime

        custom_time = datetime(2024, 6, 15, 10, 30, 0)
        key = extract_and_load.generate_parquet_key("claims", timestamp=custom_time)

        assert key.startswith("claims/")
        assert "2024/06/15" in key
        assert key.endswith(".parquet")

    @pytest.mark.L1
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
            "KAGGLE_CSV_PATH": "/data/kaggle/claims.csv",
        },
    )
    def test_run_elt_pipeline_uses_default_csv_path(
        self, mock_bq, mock_upload, mock_parquet, mock_csv, mock_postgres
    ):
        """L1: Test run_elt_pipeline uses default Kaggle CSV path when not provided."""
        from src import extract_and_load

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.return_value = b"parquet"

        result = extract_and_load.run_elt_pipeline()

        mock_csv.assert_called_with("/data/kaggle/claims.csv")
        mock_postgres.assert_called_with("SELECT * FROM customer_profiles")
        assert result["csv_extract"] is True


class TestParquetGeneration:
    """L1: Tests for Parquet generation with mocked dependencies."""

    @pytest.mark.L1
    def test_generate_parquet_key(self):
        """L1: Test generate_parquet_key function."""
        from src import extract_and_load

        key = extract_and_load.generate_parquet_key("claims")
        assert key.startswith("claims/")
        assert key.endswith(".parquet")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
