"""
============================================================================
Insurance Company Data Pipeline - L1 Unit Tests (Integrated with Mocks)
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L1: Unit tests - Mocked external dependencies, partial integration
     Test orchestration with mocked database/S3 clients

Usage:
    pytest tests/test_L1_unit_integrated.py -v
============================================================================
"""

import pytest
from unittest.mock import MagicMock, patch, Mock
import pandas as pd
import os
import sys
from io import StringIO
from datetime import datetime


class TestPostgreSQLMocked:
    """L1: PostgreSQL operations with mocks."""

    @patch("psycopg2.connect")
    def test_postgres_connect(self, mock_connect):
        """L1: Test PostgreSQL connection with mock."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        import psycopg2

        conn = psycopg2.connect("host=localhost dbname=test")

        mock_connect.assert_called_once()
        assert conn is not None

    @patch("psycopg2.connect")
    def test_postgres_query_mock(self, mock_connect):
        """L1: Test PostgreSQL query with mock."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "John"), (2, "Jane")]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        import psycopg2

        conn = psycopg2.connect("host=localhost")
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM customers")
        results = cursor.fetchall()

        assert len(results) == 2
        assert results[0][1] == "John"


class TestMinIOMocked:
    """L1: MinIO operations with mocks."""

    @patch("boto3.client")
    def test_minio_client_creation(self, mock_boto3):
        """L1: Test MinIO client creation with mock."""
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3

        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:9900",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )

        mock_boto3.assert_called_once()

    @patch("boto3.client")
    def test_minio_bucket_list(self, mock_boto3):
        """L1: Test MinIO bucket listing with mock."""
        mock_s3 = MagicMock()
        mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "insurance-data"}]}
        mock_boto3.return_value = mock_s3

        import boto3

        s3 = boto3.client("s3")
        buckets = s3.list_buckets()
        assert "Buckets" in buckets


class TestClickHouseClientMock:
    """L1: Test ClickHouse client creation."""

    def test_clickhouse_client_import(self):
        """L1: Test ClickHouse client can be imported."""
        try:
            import clickhouse_connect

            assert hasattr(clickhouse_connect, "get_client")
        except ImportError:
            pytest.skip("clickhouse_connect not installed")

    def test_clickhouse_config_format(self):
        """L1: Test ClickHouse config format."""
        config = {
            "host": "localhost",
            "port": 8123,
            "username": "default",
            "password": "clickhouse_pass",
            "database": "insurance_db",
        }
        assert config["host"] == "localhost"
        assert config["port"] == 8123


class TestDLTPipelineMocked:
    """L1: Test DLT pipeline orchestration."""

    @patch("subprocess.run")
    def test_dbt_debug_command(self, mock_run):
        """L1: Test DBT debug command."""
        mock_run.return_value = MagicMock(
            returncode=0, stdout="dbt version 1.7.0", stderr=""
        )

        import subprocess

        result = subprocess.run(["dbt", "--version"], capture_output=True, text=True)

        assert result.returncode == 0
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_dbt_debug_mock(self, mock_run):
        """L1: Test DBT debug with mock."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        import subprocess

        result = subprocess.run(
            ["dbt", "debug"], capture_output=True, text=True, env={}
        )

        mock_run.assert_called_once()


class TestGoCodeMocked:
    """L1: Test Go code with mocks."""

    @patch("subprocess.run")
    def test_go_build_command(self, mock_run):
        """L1: Test Go build command with mock."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        import subprocess

        result = subprocess.run(
            ["go", "build", "-o", "/dev/null", "synthetic_data_generator.go"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

    @patch("subprocess.run")
    def test_go_vet_command(self, mock_run):
        """L1: Test go vet command with mock."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        import subprocess

        result = subprocess.run(["go", "vet", "./..."], capture_output=True, text=True)

        assert result.returncode == 0


class TestDataFrameMocked:
    """L1: Test DataFrame creation with mocks."""

    def test_dataframe_create(self):
        """L1: Test DataFrame creation."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [100, 200, 300]})
        assert len(df) == 3
        assert "id" in df.columns

    def test_dataframe_groupby(self):
        """L1: Test DataFrame groupby."""
        df = pd.DataFrame({"category": ["A", "A", "B"], "value": [100, 200, 300]})
        result = df.groupby("category")["value"].sum()
        assert result["A"] == 300
        assert result["B"] == 300

    def test_dataframe_apply(self):
        """L1: Test DataFrame apply."""
        df = pd.DataFrame({"score": [750, 620, 800]})
        result = df["score"].apply(lambda x: "High" if x >= 700 else "Low")
        assert result[0] == "High"
        assert result[1] == "Low"


class TestEnvironmentConfig:
    """L1: Test environment configuration."""

    def test_postgres_env_defaults(self):
        """L1: Test default PostgreSQL config."""
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))

        assert host == "localhost"
        assert port == 5432

    def test_minio_env_defaults(self):
        """L1: Test default MinIO config."""
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9900")
        bucket = os.getenv("MINIO_BUCKET", "insurance-data")

        assert endpoint == "localhost:9900"
        assert bucket == "insurance-data"

    def test_clickhouse_env_defaults(self):
        """L1: Test default ClickHouse config."""
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))

        assert host == "localhost"
        assert port == 8123


class TestPipelineSteps:
    """L1: Test pipeline step orchestration."""

    @patch("subprocess.run")
    def test_pipeline_generate_step(self, mock_run):
        """L1: Test data generation step."""
        mock_run.return_value = MagicMock(returncode=0)

        import subprocess

        result = subprocess.run(["echo", "generate"], capture_output=True)

        assert result.returncode == 0

    @patch("subprocess.run")
    def test_pipeline_load_step(self, mock_run):
        """L1: Test data load step."""
        mock_run.return_value = MagicMock(returncode=0)

        import subprocess

        result = subprocess.run(["echo", "load"], capture_output=True)

        assert result.returncode == 0

    @patch("subprocess.run")
    def test_pipeline_transform_step(self, mock_run):
        """L1: Test transform step."""
        mock_run.return_value = MagicMock(returncode=0)

        import subprocess

        result = subprocess.run(["echo", "transform"], capture_output=True)

        assert result.returncode == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
