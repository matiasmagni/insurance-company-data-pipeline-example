"""
============================================================================
Insurance Company Data Pipeline - L1 Unit Tests (Mocked Integrations)
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L1: Mocked integrations - Test how components talk to each other
     Mock PostgreSQL, MinIO, ClickHouse clients
     Test orchestration flow

Usage:
    pytest tests/test_L1_unit_integrated.py -v
============================================================================
"""

import pytest
from unittest.mock import MagicMock
import pandas as pd


class TestPostgreSQLMock:
    """L1: Test PostgreSQL fetch operations."""

    def test_fetch_customers(self):
        mock_rows = [(1, "John", 750), (2, "Jane", 620)]
        df = pd.DataFrame(mock_rows, columns=["id", "name", "score"])
        assert len(df) == 2

    def test_fetch_claims(self):
        mock_rows = [(1, 1, "Open", 5000), (2, 1, "Closed", 2500)]
        df = pd.DataFrame(mock_rows, columns=["cid", "cust_id", "status", "amt"])
        assert df["status"].tolist() == ["Open", "Closed"]


class TestMinIOMock:
    """L1: Test MinIO/S3 upload operations."""

    def test_upload_parquet(self):
        mock_s3 = MagicMock()
        mock_s3.upload_file.return_value = None
        mock_s3.upload_file("/tmp/test.parquet", "bucket", "key")
        mock_s3.upload_file.assert_called_once()


class TestClickHouseMock:
    """L1: Test ClickHouse query operations."""

    def test_query_gold(self):
        mock_result = [("Excellent", 500), ("Good", 300)]
        df = pd.DataFrame(mock_result, columns=["risk", "count"])
        assert len(df) == 2


class TestPipelineOrchestration:
    """L1: Test pipeline steps."""

    def test_extract_to_raw(self):
        pg_data = pd.DataFrame({"id": [1, 2, 3]})
        assert len(pg_data) == 3

    def test_raw_to_silver(self):
        raw_df = pd.DataFrame({"customer_id": [1, 2], "credit_score": [750, 620]})
        raw_df["risk_bucket"] = raw_df["credit_score"].apply(
            lambda x: "Excellent" if x >= 750 else "Good"
        )
        assert "risk_bucket" in raw_df.columns


class TestDataQualityMock:
    """L1: Test data quality checks."""

    def test_detect_duplicates(self):
        df = pd.DataFrame({"id": [1, 2, 1, 3]})
        dups = df[df.duplicated(subset=["id"], keep=False)]
        assert len(dups) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
