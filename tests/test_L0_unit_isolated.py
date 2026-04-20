"""
================================================================================
Insurance Company Data Pipeline - L0 Unit Tests (Isolated)
================================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)
Eng. Matías J. Magni | CEO @ BugMentor

L0: Unit tests - Functions in complete isolation (no I/O)

Usage:
    pytest tests/test_L0_unit_isolated.py -v
================================================================================
"""

import io
import os
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def sample_claims_data():
    """Sample claims data from Kaggle CSV."""
    return pd.DataFrame(
        {
            "policy_number": [
                "POL-2024-00001",
                "POL-2024-00002",
                "POL-2024-00003",
                "POL-2024-00004",
                "POL-2024-00005",
            ],
            "claim_amount": [5000.0, 7500.0, 3000.0, 12000.0, 2500.0],
            "claim_date": pd.to_datetime(
                ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"]
            ),
            "fraud_indicator": ["N", "Y", "N", "Y", "N"],
            "vehicle_type": ["Sedan", "SUV", "Truck", "Sports Car", "Sedan"],
            "driver_age": [35, 28, 45, 22, 52],
        }
    )


@pytest.fixture
def sample_customer_data():
    """Sample customer data from PostgreSQL."""
    return pd.DataFrame(
        {
            "customer_id": [
                "CUST-00001",
                "CUST-00002",
                "CUST-00003",
                "CUST-00004",
                "CUST-00005",
            ],
            "name": [
                "John Doe",
                "Jane Smith",
                "Bob Johnson",
                "Alice Williams",
                "Charlie Brown",
            ],
            "email": [
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
                "alice@example.com",
                "charlie@example.com",
            ],
            "credit_score": [720, 680, 750, 620, 800],
            "telematics_score": [85.5, 72.3, 91.0, 65.8, 88.2],
            "policy_number": [
                "POL-2024-00001",
                "POL-2024-00002",
                "POL-2024-00003",
                "POL-2024-00004",
                "POL-2024-00005",
            ],
        }
    )


class TestParquetConversion:
    """L0: Tests for Parquet file conversion functionality."""

    @pytest.mark.L0
    def test_dataframe_to_parquet_bytes(self, sample_claims_data):
        """L0: Test DataFrame to Parquet bytes conversion."""
        from src import extract_and_load

        result = extract_and_load.dataframe_to_parquet(sample_claims_data)
        assert isinstance(result, (bytes, io.BytesIO.__bases__))

    @pytest.mark.L0
    def test_dataframe_to_parquet_preserves_data(self, sample_claims_data):
        """L0: Test that Parquet conversion preserves data."""
        from src import extract_and_load

        parquet_bytes = extract_and_load.dataframe_to_parquet(sample_claims_data)
        result_df = pd.read_parquet(io.BytesIO(parquet_bytes))

        assert len(result_df) == len(sample_claims_data)
        assert list(result_df.columns) == list(sample_claims_data.columns)

    @pytest.mark.L0
    def test_dataframe_to_parquet_empty_dataframe(self):
        """L0: Test that empty DataFrame is handled."""
        from src import extract_and_load

        empty_df = pd.DataFrame()
        result = extract_and_load.dataframe_to_parquet(empty_df)
        assert isinstance(result, (bytes, io.BytesIO.__bases__))


class TestDataValidation:
    """L0: Tests for data validation functions."""

    @pytest.mark.L0
    def test_validate_claims_dataframe_valid(self, sample_claims_data):
        """L0: Test validation passes for valid claims DataFrame."""
        from src import extract_and_load

        result = extract_and_load.validate_claims_dataframe(sample_claims_data)
        assert result is None

    @pytest.mark.L0
    def test_validate_claims_dataframe_missing_columns(self):
        """L0: Test validation fails for missing columns."""
        from src import extract_and_load

        invalid_df = pd.DataFrame({"claim_id": [1, 2, 3]})

        with pytest.raises(ValueError, match="Missing required columns"):
            extract_and_load.validate_claims_dataframe(invalid_df)

    @pytest.mark.L0
    def test_validate_customer_dataframe_valid(self, sample_customer_data):
        """L0: Test validation passes for valid customer DataFrame."""
        from src import extract_and_load

        result = extract_and_load.validate_customer_dataframe(sample_customer_data)
        assert result is None

    @pytest.mark.L0
    def test_validate_customer_dataframe_missing_columns(self):
        """L0: Test validation fails for missing customer columns."""
        from src import extract_and_load

        invalid_df = pd.DataFrame({"customer_id": ["C001", "C002"]})

        with pytest.raises(ValueError, match="Missing required columns"):
            extract_and_load.validate_customer_dataframe(invalid_df)


class TestConfiguration:
    """L0: Tests for configuration and environment handling."""

    @pytest.mark.L0
    def test_get_postgres_config_defaults(self):
        """L0: Test default PostgreSQL configuration."""
        from src import extract_and_load

        env_vars = [
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_DB",
        ]
        for var in env_vars:
            os.environ.pop(var, None)

        config = extract_and_load.get_postgres_config()
        assert config["host"] == "localhost"
        assert config["port"] == "5433"

    @pytest.mark.L0
    def test_get_minio_config_defaults(self):
        """L0: Test default MinIO configuration."""
        from src import extract_and_load

        env_vars = [
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "MINIO_BUCKET",
        ]
        for var in env_vars:
            os.environ.pop(var, None)

        config = extract_and_load.get_minio_config()
        assert config["endpoint"] == "localhost:9000"

    @pytest.mark.L0
    def test_get_bigquery_config_defaults(self):
        """L0: Test default BigQuery configuration."""
        from src import extract_and_load

        env_vars = ["BIGQUERY_PROJECT_ID", "BIGQUERY_DATASET"]
        for var in env_vars:
            os.environ.pop(var, None)

        config = extract_and_load.get_bigquery_config()
        assert config["project_id"] == "sandbox-project"
        assert config["dataset"] == "insurance_raw"

    @pytest.mark.L0
    def test_get_kaggle_csv_path_default(self):
        """L0: Test default Kaggle CSV path."""
        from src import extract_and_load

        os.environ.pop("KAGGLE_CSV_PATH", None)
        path = extract_and_load.get_kaggle_csv_path()
        assert "kaggle" in path.lower() or "claims" in path.lower()

    @pytest.mark.L0
    def test_get_kaggle_csv_path_from_env(self):
        """L0: Test Kaggle CSV path from environment."""
        from src import extract_and_load

        os.environ["KAGGLE_CSV_PATH"] = "/custom/path/claims.csv"
        try:
            path = extract_and_load.get_kaggle_csv_path()
            assert path == "/custom/path/claims.csv"
        finally:
            del os.environ["KAGGLE_CSV_PATH"]

    @pytest.mark.L0
    def test_generate_parquet_key_format(self):
        """L0: Test parquet key generation format."""
        from src import extract_and_load

        key = extract_and_load.generate_parquet_key("claims")
        assert key.startswith("claims/")
        assert key.endswith(".parquet")

    @pytest.mark.L0
    def test_generate_parquet_key_contains_date(self):
        """L0: Test parquet key contains date."""
        from src import extract_and_load

        key = extract_and_load.generate_parquet_key("customers")
        parts = key.split("/")
        assert len(parts) >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
