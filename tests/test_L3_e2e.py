"""
============================================================================
Insurance Company Data Pipeline - L3 End-to-End Tests
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L3: End-to-End tests - Full pipeline validation with real ClickHouse queries
     Test that data flows correctly through the entire pipeline
     Requires: docker-compose up -d and completed pipeline run

Usage:
    pytest tests/test_L3_e2e.py -v
    (Requires: docker-compose up -d and data in ClickHouse)
============================================================================
"""

import pytest
import clickhouse_connect
import os
import subprocess
from pathlib import Path


# =============================================================================
# Test Configuration
# =============================================================================

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "database": os.getenv("CLICKHOUSE_DB", "insurance_db"),
    "username": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass"),
}


# =============================================================================
# Gold Layer - ClickHouse Tests (THE REAL VALIDATION)
# =============================================================================


class TestGoldLayerClickHouse:
    """L3: Test Gold layer tables in ClickHouse with real queries."""

    @pytest.fixture
    def ch_client(self):
        """Create ClickHouse client for tests."""
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception as e:
            pytest.skip(f"ClickHouse not available: {e}")

    def test_gold_customers_exists(self, ch_client):
        """L3: Test gold_customers table exists."""
        result = ch_client.query("SHOW TABLES LIKE 'gold_customers'")
        assert len(result.result_rows) > 0, "gold_customers table does not exist"

    def test_gold_claims_exists(self, ch_client):
        """L3: Test gold_claims table exists."""
        result = ch_client.query("SHOW TABLES LIKE 'gold_claims'")
        assert len(result.result_rows) > 0, "gold_claims table does not exist"

    def test_gold_claims_by_status_exists(self, ch_client):
        """L3: Test gold_claims_by_status table exists."""
        result = ch_client.query("SHOW TABLES LIKE 'gold_claims_by_status'")
        assert len(result.result_rows) > 0, "gold_claims_by_status table does not exist"

    def test_gold_claims_by_agent_exists(self, ch_client):
        """L3: Test gold_claims_by_agent table exists."""
        result = ch_client.query("SHOW TABLES LIKE 'gold_claims_by_agent'")
        assert len(result.result_rows) > 0, "gold_claims_by_agent table does not exist"

    def test_gold_claims_by_business_line_exists(self, ch_client):
        """L3: Test gold_claims_by_business_line table exists."""
        result = ch_client.query("SHOW TABLES LIKE 'gold_claims_by_business_line'")
        assert len(result.result_rows) > 0, (
            "gold_claims_by_business_line table does not exist"
        )


class TestGoldCustomersValidation:
    """L3: Validate gold_customers table data."""

    @pytest.fixture
    def ch_client(self):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception:
            pytest.skip("ClickHouse not available")

    def test_gold_customers_has_data(self, ch_client):
        """L3: Test gold_customers has data."""
        result = ch_client.query("SELECT count() as cnt FROM gold_customers")
        count = result.result_rows[0][0]
        assert count > 0, "gold_customers has no data"

    def test_gold_customers_has_risk_bucket(self, ch_client):
        """L3: Test gold_customers has risk_bucket column."""
        result = ch_client.query("DESCRIBE gold_customers")
        columns = [row[0] for row in result.result_rows]
        assert "risk_bucket" in columns, "risk_bucket column missing"

    def test_gold_customers_risk_bucket_values(self, ch_client):
        """L3: Test risk_bucket has valid values."""
        result = ch_client.query("SELECT distinct risk_bucket FROM gold_customers")
        buckets = [row[0] for row in result.result_rows]
        expected = {"Low", "Medium", "High"}
        assert all(b in expected for b in buckets), "Invalid risk_bucket values"

    def test_gold_customers_customer_count(self, ch_client):
        """L3: Test customer count is in expected range."""
        result = ch_client.query("SELECT count() as cnt FROM gold_customers")
        count = result.result_rows[0][0]
        # Should have at least 900+ customers (from our generator)
        assert count >= 900, f"Expected ~1000 customers, got {count}"

    def test_gold_customers_credit_score_range(self, ch_client):
        """L3: Test credit scores are in valid range."""
        result = ch_client.query("""
            SELECT min(credit_score) as min_cs, max(credit_score) as max_cs 
            FROM gold_customers
        """)
        min_cs, max_cs = result.result_rows[0]
        assert 300 <= min_cs <= 850, "Invalid min credit score"
        assert 300 <= max_cs <= 850, "Invalid max credit score"


class TestGoldClaimsValidation:
    """L3: Validate gold_claims table data."""

    @pytest.fixture
    def ch_client(self):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception:
            pytest.skip("ClickHouse not available")

    def test_gold_claims_has_data(self, ch_client):
        """L3: Test gold_claims has data."""
        result = ch_client.query("SELECT count() as cnt FROM gold_claims")
        count = result.result_rows[0][0]
        assert count > 0, "gold_claims has no data"

    def test_gold_claims_has_claim_status_category(self, ch_client):
        """L3: Test gold_claims has claim_status_category."""
        result = ch_client.query("DESCRIBE gold_claims")
        columns = [row[0] for row in result.result_rows]
        assert "claim_status_category" in columns, "claim_status_category missing"

    def test_gold_claims_has_vehicle_category(self, ch_client):
        """L3: Test gold_claims has vehicle_category."""
        result = ch_client.query("DESCRIBE gold_claims")
        columns = [row[0] for row in result.result_rows]
        assert "vehicle_category" in columns, "vehicle_category missing"

    def test_gold_claims_claim_status_category_values(self, ch_client):
        """L3: Test claim_status_category has valid values."""
        result = ch_client.query(
            "SELECT distinct claim_status_category FROM gold_claims"
        )
        categories = [row[0] for row in result.result_rows]
        expected = {"Pending", "Closed", "Unknown"}
        assert all(c in expected for c in categories), (
            "Invalid claim_status_category values"
        )

    def test_gold_claims_vehicle_category_values(self, ch_client):
        """L3: Test vehicle_category has valid values."""
        result = ch_client.query("SELECT distinct vehicle_category FROM gold_claims")
        categories = [row[0] for row in result.result_rows]
        # Should contain: Sedan, SUV, Truck, Luxury, Sports, Other
        assert len(categories) > 0, "No vehicle categories"

    def test_gold_claims_count(self, ch_client):
        """L3: Test claims count is in expected range."""
        result = ch_client.query("SELECT count() as cnt FROM gold_claims")
        count = result.result_rows[0][0]
        # Should have at least 4500+ claims (from our generator)
        assert count >= 4500, f"Expected ~5000 claims, got {count}"

    def test_gold_claims_amount_range(self, ch_client):
        """L3: Test claim amounts are in valid range."""
        result = ch_client.query("""
            SELECT min(claim_amount) as min_amt, max(claim_amount) as max_amt 
            FROM gold_claims
        """)
        min_amt, max_amt = result.result_rows[0]
        assert min_amt > 0, "Invalid min claim amount"
        assert max_amt <= 10_000_000, "Claim amount too high (possible data error)"


class TestGoldClaimsByStatusValidation:
    """L3: Validate gold_claims_by_status aggregated table."""

    @pytest.fixture
    def ch_client(self):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception:
            pytest.skip("ClickHouse not available")

    def test_gold_claims_by_status_has_data(self, ch_client):
        """L3: Test gold_claims_by_status has data."""
        result = ch_client.query("SELECT count() as cnt FROM gold_claims_by_status")
        count = result.result_rows[0][0]
        assert count > 0, "gold_claims_by_status has no data"

    def test_gold_claims_by_status_columns(self, ch_client):
        """L3: Test gold_claims_by_status has required columns."""
        result = ch_client.query("DESCRIBE gold_claims_by_status")
        columns = [row[0] for row in result.result_rows]
        assert "claim_status_category" in columns
        assert "total_claims" in columns
        assert "total_amount" in columns

    def test_gold_claims_by_status_totals(self, ch_client):
        """L3: Test totals match gold_claims."""
        result = ch_client.query("""
            SELECT sum(total_claims) as total FROM gold_claims_by_status
        """)
        agg_total = result.result_rows[0][0]

        result2 = ch_client.query("SELECT count() as cnt FROM gold_claims")
        direct_count = result2.result_rows[0][0]

        assert agg_total == direct_count, "Aggregated count doesn't match"


class TestGoldClaimsByAgentValidation:
    """L3: Validate gold_claims_by_agent aggregated table."""

    @pytest.fixture
    def ch_client(self):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception:
            pytest.skip("ClickHouse not available")

    def test_gold_claims_by_agent_has_data(self, ch_client):
        """L3: Test gold_claims_by_agent has data."""
        result = ch_client.query("SELECT count() as cnt FROM gold_claims_by_agent")
        count = result.result_rows[0][0]
        assert count > 0, "gold_claims_by_agent has no data"

    def test_gold_claims_by_agent_columns(self, ch_client):
        """L3: Test gold_claims_by_agent has required columns."""
        result = ch_client.query("DESCRIBE gold_claims_by_agent")
        columns = [row[0] for row in result.result_rows]
        assert "agent_id" in columns
        assert "total_claims" in columns
        assert "total_amount" in columns
        assert "avg_claim_amount" in columns


class TestGoldClaimsByBusinessLineValidation:
    """L3: Validate gold_claims_by_business_line aggregated table."""

    @pytest.fixture
    def ch_client(self):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception:
            pytest.skip("ClickHouse not available")

    def test_gold_claims_by_business_line_has_data(self, ch_client):
        """L3: Test gold_claims_by_business_line has data."""
        result = ch_client.query(
            "SELECT count() as cnt FROM gold_claims_by_business_line"
        )
        count = result.result_rows[0][0]
        assert count > 0, "gold_claims_by_business_line has no data"

    def test_gold_claims_by_business_line_columns(self, ch_client):
        """L3: Test gold_claims_by_business_line has required columns."""
        result = ch_client.query("DESCRIBE gold_claims_by_business_line")
        columns = [row[0] for row in result.result_rows]
        assert "business_line" in columns
        assert "total_claims" in columns
        assert "total_amount" in columns

    def test_gold_claims_by_business_line_business_lines(self, ch_client):
        """L3: Test business lines are valid."""
        result = ch_client.query(
            "SELECT distinct business_line FROM gold_claims_by_business_line"
        )
        lines = [row[0] for row in result.result_rows]
        expected = {"Auto", "Home", "Life", "Health"}
        assert all(l in expected for l in lines), "Invalid business lines"


class TestFullPipelineValidation:
    """L3: Validate complete end-to-end data flow."""

    @pytest.fixture
    def ch_client(self):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG["host"],
                port=CLICKHOUSE_CONFIG["port"],
                username=CLICKHOUSE_CONFIG["username"],
                password=CLICKHOUSE_CONFIG["password"],
                database=CLICKHOUSE_CONFIG["database"],
            )
            yield client
            client.close()
        except Exception:
            pytest.skip("ClickHouse not available")

    def test_pipeline_data_balance(self, ch_client):
        """L3: Test data flows correctly without loss."""
        # Get counts from all gold tables
        customers = ch_client.query("SELECT count() FROM gold_customers").result_rows[
            0
        ][0]
        claims = ch_client.query("SELECT count() FROM gold_claims").result_rows[0][0]

        # Should have ~1000 customers and ~5000 claims
        assert customers >= 900, f"Lost customers: expected ~1000, got {customers}"
        assert claims >= 4500, f"Lost claims: expected ~5000, got {claims}"

    def test_data_transformation_complete(self, ch_client):
        """L3: Test all transformations are applied."""
        # Check silver columns exist in gold
        result = ch_client.query("DESCRIBE gold_claims")
        columns = [row[0] for row in result.result_rows]

        assert "risk_bucket" in columns, "risk_bucket not transformed to gold"
        assert "claim_status_category" in columns, "claim_status_category not in gold"
        assert "vehicle_category" in columns, "vehicle_category not in gold"

    def test_aggregation_quality(self, ch_client):
        """L3: Test aggregations are mathematically correct."""
        # Total amounts should match sum of individual claims
        result = ch_client.query("""
            SELECT sum(total_amount) as total FROM gold_claims_by_status
        """)
        by_status_total = result.result_rows[0][0]

        result2 = ch_client.query("""
            SELECT sum(claim_amount) as total FROM gold_claims
        """)
        direct_total = result2.result_rows[0][0]

        # Allow for small floating point differences
        assert abs(by_status_total - direct_total) < 1, "Aggregation math error"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
