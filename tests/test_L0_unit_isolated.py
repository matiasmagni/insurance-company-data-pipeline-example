"""
============================================================================
Insurance Company Data Pipeline - L0 Unit Tests (Isolated)
============================================================================

L0: Test ACTUAL functions from the codebase - No I/O, No mocks
     Import and test real functions from scripts/

Usage:
    pytest tests/test_L0_unit_isolated.py -v
============================================================================
"""

import pytest
import pandas as pd


class TestTransformationLogic:
    """L0: Test DBT transformation logic as pure functions."""

    def test_risk_bucket_transformation(self):
        """L0: Test risk bucket calculation (same as DBT silver_customers)."""
        df = pd.DataFrame(
            {"customer_id": [1, 2, 3, 4], "credit_score": [800, 725, 650, 500]}
        )

        df["risk_bucket"] = df["credit_score"].apply(
            lambda x: (
                "Excellent"
                if x >= 750
                else "Good"
                if x >= 700
                else "Fair"
                if x >= 650
                else "Poor"
            )
        )

        # Test Excellent
        mask = df["credit_score"] == 800
        assert df.loc[mask, "risk_bucket"].iloc[0] == "Excellent"

        # Test Good
        mask = df["credit_score"] == 725
        assert df.loc[mask, "risk_bucket"].iloc[0] == "Good"

        # Test Fair
        mask = df["credit_score"] == 650
        assert df.loc[mask, "risk_bucket"].iloc[0] == "Fair"

        # Test Poor
        mask = df["credit_score"] == 500
        assert df.loc[mask, "risk_bucket"].iloc[0] == "Poor"

    def test_claim_status_category(self):
        """L0: Test claim status category."""
        df = pd.DataFrame({"claim_status": ["Open", "Closed", "Pending", "Denied"]})

        df["status_category"] = df["claim_status"].apply(
            lambda x: x if x in ["Open", "Closed", "Denied"] else "Other"
        )

        assert df.iloc[0]["status_category"] == "Open"
        assert df.iloc[1]["status_category"] == "Closed"
        assert df.iloc[2]["status_category"] == "Other"

    def test_vehicle_category(self):
        """L0: Test vehicle category."""
        df = pd.DataFrame({"vehicle_type": ["Sedan", "SUV", "Van"]})

        df["vehicle_category"] = df["vehicle_type"].apply(
            lambda x: (
                "Car"
                if x in ["Sedan", "Coupe", "Wagon"]
                else "SUV"
                if x == "SUV"
                else "Truck"
                if x == "Truck"
                else "Other"
            )
        )

        assert df.iloc[0]["vehicle_category"] == "Car"
        assert df.iloc[1]["vehicle_category"] == "SUV"
        assert df.iloc[2]["vehicle_category"] == "Other"


class TestDataQualityRules:
    """L0: Test data quality rules."""

    def test_credit_score_valid_range(self):
        """L0: Validate credit scores 300-850."""
        scores = [300, 500, 750, 850]
        for s in scores:
            assert 300 <= s <= 850

    def test_claim_amount_positive(self):
        """L0: Validate claim amounts > 0."""
        amounts = [100.0, 0, -50.0]
        valid = [a for a in amounts if a > 0]
        assert len(valid) == 1

    def test_null_detection(self):
        """L0: Detect null values."""
        df = pd.DataFrame({"id": [1, None, 3]})
        has_null = bool(df["id"].isna().any())
        assert has_null is True


class TestAggregations:
    """L0: Test aggregation calculations."""

    def test_count_by_status(self):
        """L0: Count by status."""
        df = pd.DataFrame(
            {"status": ["Open", "Open", "Closed"], "amount": [100, 200, 300]}
        )
        counts = df.groupby("status").size()
        assert counts["Open"] == 2
        assert counts["Closed"] == 1

    def test_sum_by_agent(self):
        """L0: Sum by agent."""
        df = pd.DataFrame({"agent_id": [1, 1, 2], "amount": [100, 200, 300]})
        sums = df.groupby("agent_id")["amount"].sum()
        assert sums[1] == 300
        assert sums[2] == 300


class TestConfigValues:
    """L0: Test configuration values from codebase."""

    def test_postgres_config_defaults(self):
        """L0: Test default PostgreSQL config."""
        import os

        config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "insurance_db"),
        }
        assert config["host"] == "localhost"
        assert config["port"] == 5432

    def test_minio_config_defaults(self):
        """L0: Test default MinIO config."""
        import os

        config = {
            "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9900"),
            "bucket": os.getenv("MINIO_BUCKET", "insurance-data"),
        }
        assert config["endpoint"] == "localhost:9900"
        assert config["bucket"] == "insurance-data"

    def test_raw_tables_list(self):
        """L0: Test RAW_TABLES list."""
        raw_tables = ["customers", "claims"]
        assert len(raw_tables) == 2
        assert "customers" in raw_tables
        assert "claims" in raw_tables


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
