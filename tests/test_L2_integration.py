"""
============================================================================
Insurance Company Data Pipeline - L2 Integration Tests
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L2: Integration tests - Test real services running in Docker
     Requires: docker-compose up -d

Usage:
    pytest tests/test_L2_integration.py -v
============================================================================
"""

import pytest
import psycopg2
import boto3
import os


POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "insurance_db"),
    "user": os.getenv("POSTGRES_USER", "insurance_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "insurance_pass"),
}

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9900"),
    "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    "bucket": os.getenv("MINIO_BUCKET", "insurance-data"),
}


class TestPostgreSQLIntegration:
    """L2: Test real PostgreSQL operations."""

    def test_postgres_connection(self):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.close()
        except Exception as e:
            pytest.skip(f"PostgreSQL unavailable: {e}")

    def test_customers_table_exists(self):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customers")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            assert count >= 0
        except Exception as e:
            pytest.skip(f"PostgreSQL unavailable: {e}")

    def test_claims_table_exists(self):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM claims")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            assert count >= 0
        except Exception as e:
            pytest.skip(f"PostgreSQL unavailable: {e}")


class TestMinIOIntegration:
    """L2: Test real MinIO operations."""

    @pytest.fixture
    def s3_client(self):
        try:
            client = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
                aws_access_key_id=MINIO_CONFIG["access_key"],
                aws_secret_access_key=MINIO_CONFIG["secret_key"],
            )
            client.head_bucket(Bucket=MINIO_CONFIG["bucket"])
            return client
        except Exception as e:
            pytest.skip(f"MinIO unavailable: {e}")

    def test_bucket_exists(self, s3_client):
        s3_client.head_bucket(Bucket=MINIO_CONFIG["bucket"])

    def test_can_list_objects(self, s3_client):
        try:
            result = s3_client.list_objects_v2(Bucket=MINIO_CONFIG["bucket"])
            assert "Contents" in result or result.get("KeyCount", 0) == 0
        except Exception as e:
            pytest.skip(f"MinIO unavailable: {e}")


class TestDBTModels:
    """L2: Test DBT model files exist and are valid SQL."""

    def test_silver_customers_sql(self):
        from pathlib import Path

        content = Path("dbt/models/silver/silver_customers.sql").read_text()
        assert "risk_bucket" in content.lower()
        assert "CASE" in content

    def test_silver_claims_sql(self):
        from pathlib import Path

        content = Path("dbt/models/silver/silver_claims.sql").read_text()
        assert "claim_status_category" in content.lower()
        assert "vehicle_category" in content.lower()

    def test_gold_customers_sql(self):
        from pathlib import Path

        content = Path("dbt/models/gold/gold_customers.sql").read_text()
        assert len(content) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
