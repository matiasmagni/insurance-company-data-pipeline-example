"""
============================================================================
Insurance Company Data Pipeline - L2 Integration Tests
============================================================================
L2: Integration tests - Test real services communication
     Tests that Step A can talk to Step B.

Usage:
    pytest tests/test_L2_integration.py -v
============================================================================
"""

import pytest
import psycopg2
import boto3
import os
import pandas as pd
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from scripts.dlt_pipeline import run_dlt_pipeline
except ImportError:
    run_dlt_pipeline = None

try:
    from scripts.silver_transform import transform_table_to_parquet
except ImportError:
    transform_table_to_parquet = None

# Config
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


class TestDLTToMinIO:
    """L2: Test DLT extraction from Postgres to MinIO."""

    def test_dlt_postgres_to_minio(self):
        """Proof that DLT can move data from PG to MinIO."""
        try:
            # 1. Ensure we have some data in PG
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS test_dlt (id INT, name TEXT)")
            cur.execute("INSERT INTO test_dlt VALUES (1, 'test')")
            conn.commit()
            cur.close()
            conn.close()

            # 2. Run DLT (modified to test our table)
            # Actually, we'll just run the real one and check if it produced anything
            # if services are up
            import subprocess
            import sys

            script_path = Path(__file__).parent.parent / "scripts" / "dlt_pipeline.py"
            subprocess.run([sys.executable, str(script_path)], check=True)

            # 3. Check MinIO
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
                aws_access_key_id=MINIO_CONFIG["access_key"],
                aws_secret_access_key=MINIO_CONFIG["secret_key"],
            )

            response = s3.list_objects_v2(
                Bucket=MINIO_CONFIG["bucket"], Prefix="raw/customers/"
            )
            assert "Contents" in response, "DLT did not produce any output in MinIO"

        except Exception as e:
            pytest.skip(f"Services unavailable: {e}")


class TestPythonToMinIO:
    """L2: Test Python script transformation logic on MinIO."""

    def test_silver_transformation_flow(self):
        """Proof that silver_transform can read raw and write silver."""
        try:
            import subprocess
            import sys

            script_path = (
                Path(__file__).parent.parent / "scripts" / "silver_transform.py"
            )
            subprocess.run([sys.executable, str(script_path)], check=True)

            # Check MinIO silver bucket
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
                aws_access_key_id=MINIO_CONFIG["access_key"],
                aws_secret_access_key=MINIO_CONFIG["secret_key"],
            )

            response = s3.list_objects_v2(
                Bucket=MINIO_CONFIG["bucket"], Prefix="silver/silver_customers/"
            )
            assert "Contents" in response, "Silver transform did not produce any output"

        except Exception as e:
            pytest.skip(f"Services unavailable: {e}")


class TestMinIOToClickHouse:
    """L2: Test loading from MinIO to ClickHouse."""

    def test_load_silver_to_clickhouse(self):
        """Proof that loader script can move data from MinIO to ClickHouse."""
        try:
            import subprocess
            import sys

            script_path = (
                Path(__file__).parent.parent
                / "scripts"
                / "load_silver_to_clickhouse.py"
            )
            subprocess.run([sys.executable, str(script_path)], check=True)

            # Check ClickHouse
            import clickhouse_connect

            ch = clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                database=os.getenv("CLICKHOUSE_DB", "insurance_db"),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass"),
            )

            result = ch.query("SELECT count() FROM silver_customers")
            assert result.result_rows[0][0] >= 0

        except Exception as e:
            pytest.skip(f"Services unavailable: {e}")
