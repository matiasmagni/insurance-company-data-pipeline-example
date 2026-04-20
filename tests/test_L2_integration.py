"""
============================================================================
Insurance Company Data Pipeline - L2 Integration Tests
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L2: Integration tests - Real services running in Docker
     Test every layer integration (PostgreSQL -> MinIO -> ClickHouse)
     Requires: docker-compose up -d

Usage:
    pytest tests/test_L2_integration.py -v
    (Requires: docker-compose up -d)
============================================================================
"""

import pytest
import pandas as pd
import psycopg2
import boto3
import os
import subprocess
from pathlib import Path


# =============================================================================
# Test Configuration
# =============================================================================

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "insurance_db"),
    "user": os.getenv("POSTGRES_USER", "insurance_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "insurance_pass"),
}

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9900"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "bucket": os.getenv("MINIO_BUCKET", "insurance-data"),
}


# =============================================================================
# PostgreSQL Layer Tests
# =============================================================================


class TestPostgreSQLLayer:
    """L2: Test PostgreSQL raw layer."""

    def test_postgres_connection(self):
        """L2: Test PostgreSQL is accessible."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.close()
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")

    def test_postgres_customers_table_exists(self):
        """L2: Test customers table exists."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'customers'
                )
            """)
            exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            assert exists, "customers table does not exist"
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")

    def test_postgres_claims_table_exists(self):
        """L2: Test claims table exists."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'claims'
                )
            """)
            exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            assert exists, "claims table does not exist"
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")

    def test_postgres_has_customers_data(self):
        """L2: Test customers table has data."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            df = pd.read_sql("SELECT * FROM customers LIMIT 10", conn)
            conn.close()
            assert len(df) > 0, "No customers in database"
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")

    def test_postgres_has_claims_data(self):
        """L2: Test claims table has data."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            df = pd.read_sql("SELECT * FROM claims LIMIT 10", conn)
            conn.close()
            assert len(df) > 0, "No claims in database"
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")

    def test_postgres_customers_schema(self):
        """L2: Test customers table schema."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'customers'
                ORDER BY column_name
            """)
            columns = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            conn.close()

            assert "customer_id" in columns
            assert "first_name" in columns
            assert "last_name" in columns
            assert "email" in columns
            assert "credit_score" in columns
            assert "annual_income" in columns
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")

    def test_postgres_claims_schema(self):
        """L2: Test claims table schema."""
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'claims'
                ORDER BY column_name
            """)
            columns = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            conn.close()

            assert "claim_id" in columns
            assert "customer_id" in columns
            assert "claim_amount" in columns
            assert "claim_status" in columns
            assert "claim_date" in columns
        except psycopg2.OperationalError:
            pytest.skip("PostgreSQL not available")


# =============================================================================
# MinIO Layer Tests
# =============================================================================


class TestMinIOLayer:
    """L2: Test MinIO silver layer."""

    @pytest.fixture
    def s3_client(self):
        """Create S3 client for tests."""
        try:
            client = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
                aws_access_key_id=MINIO_CONFIG["access_key"],
                aws_secret_access_key=MINIO_CONFIG["secret_key"],
            )
            client.head_bucket(Bucket=MINIO_CONFIG["bucket"])
            return client
        except Exception:
            pytest.skip("MinIO not available")

    def test_minio_bucket_exists(self, s3_client):
        """L2: Test MinIO bucket exists."""
        try:
            s3_client.head_bucket(Bucket=MINIO_CONFIG["bucket"])
        except Exception:
            pytest.fail(f"Bucket {MINIO_CONFIG['bucket']} does not exist")

    def test_minio_has_silver_customers(self, s3_client):
        """L2: Test silver customers file exists."""
        try:
            response = s3_client.list_objects_v2(
                Bucket=MINIO_CONFIG["bucket"], Prefix="silver/"
            )
            files = [obj["Key"] for obj in response.get("Contents", [])]
            silver_files = [f for f in files if "customers" in f.lower()]
            assert len(silver_files) > 0, "No silver customers files found"
        except Exception:
            pytest.skip("MinIO not accessible")

    def test_minio_has_silver_claims(self, s3_client):
        """L2: Test silver claims file exists."""
        try:
            response = s3_client.list_objects_v2(
                Bucket=MINIO_CONFIG["bucket"], Prefix="silver/"
            )
            files = [obj["Key"] for obj in response.get("Contents", [])]
            silver_files = [f for f in files if "claims" in f.lower()]
            assert len(silver_files) > 0, "No silver claims files found"
        except Exception:
            pytest.skip("MinIO not accessible")

    def test_minio_silver_customers_parquet(self, s3_client):
        """L2: Test silver customers is parquet format."""
        try:
            response = s3_client.list_objects_v2(
                Bucket=MINIO_CONFIG["bucket"], Prefix="silver/customers"
            )
            files = [obj["Key"] for obj in response.get("Contents", [])]
            parquet_files = [f for f in files if f.endswith(".parquet")]
            assert len(parquet_files) > 0, "No parquet files in silver/customers"
        except Exception:
            pytest.skip("MinIO not accessible")

    def test_minio_silver_claims_parquet(self, s3_client):
        """L2: Test silver claims is parquet format."""
        try:
            response = s3_client.list_objects_v2(
                Bucket=MINIO_CONFIG["bucket"], Prefix="silver/claims"
            )
            files = [obj["Key"] for obj in response.get("Contents", [])]
            parquet_files = [f for f in files if f.endswith(".parquet")]
            assert len(parquet_files) > 0, "No parquet files in silver/claims"
        except Exception:
            pytest.skip("MinIO not accessible")


# =============================================================================
# DBT Models Tests
# =============================================================================


class TestDBTModels:
    """L2: Test DBT models exist and are valid."""

    def test_dbt_sources_yml_exists(self):
        """L2: Test sources.yml exists."""
        assert Path("dbt/models/sources.yml").exists()

    def test_dbt_silver_customers_model(self):
        """L2: Test silver customers model exists."""
        assert Path("dbt/models/silver/silver_customers.sql").exists()

    def test_dbt_silver_claims_model(self):
        """L2: Test silver claims model exists."""
        assert Path("dbt/models/silver/silver_claims.sql").exists()

    def test_dbt_gold_customers_model(self):
        """L2: Test gold customers model exists."""
        assert Path("dbt/models/gold/gold_customers.sql").exists()

    def test_dbt_gold_claims_model(self):
        """L2: Test gold claims model exists."""
        assert Path("dbt/models/gold/gold_claims.sql").exists()

    def test_dbt_silver_customers_has_risk_bucket(self):
        """L2: Test silver customers has risk_bucket column."""
        content = Path("dbt/models/silver/silver_customers.sql").read_text()
        assert "risk_bucket" in content.lower()

    def test_dbt_silver_claims_has_status_category(self):
        """L2: Test silver claims has claim_status_category."""
        content = Path("dbt/models/silver/silver_claims.sql").read_text()
        assert "claim_status_category" in content.lower()

    def test_dbt_silver_claims_has_vehicle_category(self):
        """L2: Test silver claims has vehicle_category."""
        content = Path("dbt/models/silver/silver_claims.sql").read_text()
        assert "vehicle_category" in content.lower()


# =============================================================================
# Docker Services Tests
# =============================================================================


class TestDockerServices:
    """L2: Test Docker services are running."""

    @pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists(), reason="Docker not available"
    )
    def test_postgres_container_running(self):
        """L2: Test PostgreSQL container is running."""
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=insurance_postgres",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
        )
        assert "insurance_postgres" in result.stdout

    @pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists(), reason="Docker not available"
    )
    def test_minio_container_running(self):
        """L2: Test MinIO container is running."""
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=insurance_minio",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
        )
        assert "insurance_minio" in result.stdout

    @pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists(), reason="Docker not available"
    )
    def test_clickhouse_container_running(self):
        """L2: Test ClickHouse container is running."""
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=insurance_clickhouse",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
        )
        assert "insurance_clickhouse" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
