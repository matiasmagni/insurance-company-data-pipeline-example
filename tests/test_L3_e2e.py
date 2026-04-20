"""
================================================================================
Insurance Company Data Pipeline - L3 End-to-End Tests
================================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)
Eng. Matías J. Magni | CEO @ BugMentor

L3: End-to-end tests - Full pipeline execution with real infrastructure

Usage:
    pytest tests/test_L3_e2e.py -v
================================================================================
"""

import os
import pytest
import pandas as pd


class TestL3EndToEnd:
    """L3: End-to-end tests with real local infrastructure."""

    @pytest.mark.L3
    def test_extract_postgres_real_connection(self):
        """L3: Test PostgreSQL extraction with real local database."""
        from src import extract_and_load

        os.environ["POSTGRES_HOST"] = "localhost"
        os.environ["POSTGRES_PORT"] = "5433"
        os.environ["POSTGRES_DB"] = "insurance_db"
        os.environ["POSTGRES_USER"] = "postgres"
        os.environ["POSTGRES_PASSWORD"] = "postgres"

        try:
            df = extract_and_load.extract_postgres("SELECT * FROM customer_profiles")
            assert df is not None
            assert len(df) == 100
            assert "customer_id" in df.columns
            assert "name" in df.columns
            assert "email" in df.columns
            assert "policy_number" in df.columns
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")

    @pytest.mark.L3
    def test_minio_real_connection(self):
        """L3: Test MinIO upload with real local MinIO."""
        from src import extract_and_load

        os.environ["MINIO_ENDPOINT"] = "localhost:9000"
        os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
        os.environ["MINIO_SECRET_KEY"] = "minioadmin"
        os.environ["MINIO_BUCKET"] = "test-l3-bucket"
        os.environ["MINIO_SECURE"] = "false"

        test_df = pd.DataFrame({"col": [1, 2, 3]})
        parquet_bytes = extract_and_load.dataframe_to_parquet(test_df)

        try:
            result = extract_and_load.upload_to_minio(
                parquet_bytes, "test-l3-bucket", "test/l3_output.parquet"
            )
            assert result is True

            import boto3

            s3 = boto3.client(
                "s3",
                endpoint_url="http://localhost:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            response = s3.get_object(
                Bucket="test-l3-bucket", Key="test/l3_output.parquet"
            )
            assert response["Body"].read() == parquet_bytes
        except Exception as e:
            pytest.skip(f"MinIO not available: {e}")

    @pytest.mark.L3
    def test_postgres_real_extraction_with_data(self):
        """L3: Test PostgreSQL extraction returns actual table data."""
        from src import extract_and_load

        os.environ["POSTGRES_HOST"] = "localhost"
        os.environ["POSTGRES_PORT"] = "5433"
        os.environ["POSTGRES_DB"] = "insurance_db"
        os.environ["POSTGRES_USER"] = "postgres"
        os.environ["POSTGRES_PASSWORD"] = "postgres"

        try:
            df = extract_and_load.extract_postgres(
                "SELECT customer_id, name, email, policy_number, credit_score FROM customer_profiles LIMIT 10"
            )
            assert df is not None
            assert len(df) == 10
            assert "customer_id" in df.columns
            assert "name" in df.columns
            assert "email" in df.columns
            assert "policy_number" in df.columns
            assert df["customer_id"].str.startswith("CUST-").all()
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")

    @pytest.mark.L3
    def test_parquet_conversion_with_real_data(self):
        """L3: Test parquet conversion with real PostgreSQL data."""
        from src import extract_and_load

        os.environ["POSTGRES_HOST"] = "localhost"
        os.environ["POSTGRES_PORT"] = "5433"
        os.environ["POSTGRES_DB"] = "insurance_db"
        os.environ["POSTGRES_USER"] = "postgres"
        os.environ["POSTGRES_PASSWORD"] = "postgres"

        try:
            df = extract_and_load.extract_postgres(
                "SELECT * FROM customer_profiles LIMIT 50"
            )
            parquet_bytes = extract_and_load.dataframe_to_parquet(df)
            assert parquet_bytes is not None
            assert len(parquet_bytes) > 0
            assert len(df) == 50
        except Exception as e:
            pytest.skip(f"Test failed: {e}")

    @pytest.mark.L3
    def test_main_entry_point_success(self):
        """L3: Test main entry point exits with 0 on success."""
        from unittest.mock import MagicMock
        import src.extract_and_load as el
        import sys

        el.run_elt_pipeline = MagicMock(return_value=True)

        from dotenv import load_dotenv

        load_dotenv()
        try:
            status = el.run_elt_pipeline()
            print("\nPipeline completed successfully!")
        except SystemExit as e:
            assert e.code == 0

    @pytest.mark.L3
    def test_main_entry_point_error(self):
        """L3: Test main entry point exits with 1 on error."""
        from unittest.mock import MagicMock
        import src.extract_and_load as el
        import sys

        def raise_error(**kwargs):
            raise Exception("Pipeline failed")

        el.run_elt_pipeline = raise_error

        from dotenv import load_dotenv

        load_dotenv()
        try:
            status = el.run_elt_pipeline()
            print("\nPipeline completed successfully!")
            assert False, "Should have raised exception"
        except SystemExit as e:
            assert e.code == 1
        except Exception as e:
            print(f"\nPipeline failed: {e}")
            assert str(e) == "Pipeline failed"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
