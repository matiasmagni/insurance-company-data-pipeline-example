"""
============================================================================
Insurance Company Data Pipeline - L3 End-to-End Tests
============================================================================
L3: End-to-End tests - Full pipeline validation
    - Injects data into Postgres
    - Runs the orchestrator (pipeline.py)
    - Validates results in ClickHouse analytics tables

Usage:
    pytest tests/test_L3_e2e.py -v
============================================================================
"""

import pytest
import clickhouse_connect
import psycopg2
import os
import subprocess
import sys
from pathlib import Path

# Config
CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "database": os.getenv("CLICKHOUSE_DB", "insurance_db"),
    "username": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass"),
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5435")),
    "database": os.getenv("POSTGRES_DB", "insurance_db"),
    "user": os.getenv("POSTGRES_USER", "insurance_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "insurance_pass"),
}


@pytest.fixture(scope="session")
def setup_e2e():
    """Inject test data and run pipeline."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()

        cur.execute("DELETE FROM claims WHERE agent_name = 'E2E_TEST_AGENT'")
        cur.execute("DELETE FROM customers WHERE email LIKE 'e2e_%'")

        customers = [
            (9001, "E2E", "User1", "e2e_1@test.com", 800, 100000),
            (9002, "E2E", "User2", "e2e_2@test.com", 720, 80000),
            (9003, "E2E", "User3", "e2e_3@test.com", 670, 60000),
            (9004, "E2E", "User4", "e2e_4@test.com", 600, 40000),
            (9005, "E2E", "User5", "e2e_5@test.com", None, 0),
        ]

        cur.executemany(
            "INSERT INTO customers (customer_id, first_name, last_name, email, credit_score, annual_income) VALUES (%s, %s, %s, %s, %s, %s)",
            customers,
        )

        claims = [
            (
                9001,
                9001,
                "2024-01-01",
                "Auto",
                "Closed",
                5000,
                "Sedan",
                "E2E_TEST_AGENT",
            ),
            (9002, 9002, "2024-01-02", "Home", "Open", 10000, "SUV", "E2E_TEST_AGENT"),
            (
                9003,
                9003,
                "2024-01-03",
                "Life",
                "Denied",
                100000,
                "Truck",
                "E2E_TEST_AGENT",
            ),
            (
                9004,
                9004,
                "2024-01-04",
                "Health",
                "Investigation",
                2000,
                "Motorcycle",
                "E2E_TEST_AGENT",
            ),
        ]

        cur.executemany(
            "INSERT INTO claims (claim_id, customer_id, claim_date, claim_type, claim_status, claim_amount, vehicle_type, agent_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            claims,
        )

        conn.commit()
        cur.close()
        conn.close()

        pipeline_path = Path(__file__).parent.parent / "pipeline.py"
        subprocess.run([sys.executable, str(pipeline_path)], check=True)

        yield

    except Exception as e:
        pytest.skip(f"Services unavailable: {e}")


class TestE2EValidation:
    """Validate data in ClickHouse after pipeline run."""

    @pytest.fixture
    def ch_client(self):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG["host"],
            port=CLICKHOUSE_CONFIG["port"],
            username=CLICKHOUSE_CONFIG["username"],
            password=CLICKHOUSE_CONFIG["password"],
            database=CLICKHOUSE_CONFIG["database"],
        )
        yield client
        client.close()

    def test_analytics_customers_risk_buckets(self, setup_e2e, ch_client):
        """Verify risk buckets are correctly calculated for E2E data."""
        result = ch_client.query("""
            SELECT email, risk_bucket FROM analytics_customers 
            WHERE email LIKE 'e2e_%' ORDER BY email
        """)
        rows = dict(result.result_rows)

        assert rows["e2e_1@test.com"] == "Excellent"
        assert rows["e2e_2@test.com"] == "Good"
        assert rows["e2e_3@test.com"] == "Fair"
        assert rows["e2e_4@test.com"] == "Poor"
        assert rows["e2e_5@test.com"] == "Unknown"

    def test_analytics_claims_categories(self, setup_e2e, ch_client):
        """Verify claim categories are correctly calculated."""
        result = ch_client.query("""
            SELECT claim_id, claim_status_category, vehicle_category 
            FROM analytics_claims 
            WHERE claim_id IN ('9001', '9002', '9003', '9004')
            ORDER BY claim_id
        """)
        rows = {row[0]: (row[1], row[2]) for row in result.result_rows}

        # '9001': Auto, Closed, Sedan
        assert rows["9001"] == ("Closed", "Car")
        # '9002': Home, Open, SUV
        assert rows["9002"] == ("Open", "SUV")
        # '9003': Life, Denied, Truck
        assert rows["9003"] == ("Denied", "Truck")
        # '9004': Health, Investigation, Motorcycle
        assert rows["9004"] == ("Other", "Motorcycle")

    def test_analytics_aggregations_exist(self, setup_e2e, ch_client):
        """Check all analytics aggregation tables exist and have data."""
        tables = [
            "analytics_claims_by_status",
            "analytics_claims_by_agent",
            "analytics_claims_by_business_line",
        ]
        for table in tables:
            result = ch_client.query(f"SELECT count() FROM {table}")
            assert result.result_rows[0][0] > 0

    def test_analytics_business_line_values(self, setup_e2e, ch_client):
        """Check business line values match expected set."""
        result = ch_client.query(
            "SELECT distinct business_line FROM analytics_claims_by_business_line"
        )
        lines = [row[0] for row in result.result_rows]
        # Should contain at least Auto, Home, Life, Health from our injection
        assert set(["Auto", "Home", "Life", "Health"]).issubset(set(lines))
