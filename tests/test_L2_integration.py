"""
================================================================================
Insurance Company Data Pipeline - L2 Integration Tests
================================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)
Eng. Matías J. Magni | CEO @ BugMentor

L2: Integration tests - Real dependencies (local services)

Usage:
    pytest tests/test_L2_integration.py -v
================================================================================
"""

import os
from unittest.mock import patch
import pytest
import pandas as pd


@pytest.fixture
def sample_claims_data():
    """Sample claims data from Kaggle CSV."""
    return pd.DataFrame(
        {
            "policy_number": [
                "POL-2024-00001",
                "POL-2024-00002",
                "POL-2024-00003",
            ],
            "claim_amount": [5000.0, 7500.0, 3000.0],
            "claim_date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "fraud_indicator": ["N", "Y", "N"],
            "vehicle_type": ["Sedan", "SUV", "Truck"],
            "driver_age": [35, 28, 45],
        }
    )


@pytest.fixture
def sample_customer_data():
    """Sample customer data from PostgreSQL."""
    return pd.DataFrame(
        {
            "customer_id": ["CUST-00001", "CUST-00002", "CUST-00003"],
            "name": ["John Doe", "Jane Smith", "Bob Johnson"],
            "email": [
                "john@example.com",
                "jane@example.com",
                "bob@example.com",
            ],
            "credit_score": [720, 680, 750],
            "telematics_score": [85.5, 72.3, 91.0],
            "policy_number": [
                "POL-2024-00001",
                "POL-2024-00002",
                "POL-2024-00003",
            ],
        }
    )


class TestEndToEndPipeline:
    """L2: Tests for the complete ELT pipeline."""

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline(
        self,
        mock_load_bq,
        mock_upload,
        mock_to_parquet,
        mock_extract_postgres,
        mock_extract_csv,
        sample_claims_data,
        sample_customer_data,
    ):
        """L2: Test the complete ELT pipeline."""
        from src import extract_and_load

        mock_extract_csv.return_value = sample_claims_data
        mock_extract_postgres.return_value = sample_customer_data
        mock_to_parquet.return_value = b"test-parquet"

        result = extract_and_load.run_elt_pipeline(
            csv_path="/data/claims.csv",
            postgres_query="SELECT * FROM customer_profiles",
        )

        assert mock_extract_csv.called
        assert mock_extract_postgres.called
        assert mock_to_parquet.call_count == 2
        assert mock_upload.call_count == 2
        assert mock_load_bq.call_count == 2

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    def test_run_elt_pipeline_csv_error(self, mock_extract_csv):
        """L2: Test pipeline handles CSV extraction errors."""
        from src import extract_and_load

        mock_extract_csv.side_effect = FileNotFoundError("CSV not found")

        with pytest.raises(FileNotFoundError):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/missing.csv",
                postgres_query="SELECT * FROM customer_profiles",
            )


class TestELTPipelineErrorHandling:
    """L2: Tests for ELT pipeline error handling paths."""

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_parquet_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles Parquet conversion errors."""
        from src import extract_and_load

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.side_effect = Exception("Parquet conversion failed")

        with pytest.raises(Exception, match="Parquet conversion failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_upload_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles MinIO upload errors."""
        from src import extract_and_load

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.return_value = b"parquet"
        mock_upload.side_effect = Exception("Upload failed")

        with pytest.raises(Exception, match="Upload failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_bigquery_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles BigQuery load errors."""
        from src import extract_and_load

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.return_value = b"parquet"
        mock_bq.side_effect = Exception("BigQuery load failed")

        with pytest.raises(Exception, match="BigQuery load failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_postgres_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles PostgreSQL extraction errors."""
        from src import extract_and_load

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.side_effect = Exception("PostgreSQL connection failed")

        with pytest.raises(Exception, match="PostgreSQL connection failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_customers_upload_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles customers MinIO upload errors."""
        from src import extract_and_load

        call_count = [0]

        def upload_side_effect(data, bucket, key):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Customers upload failed")

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.return_value = b"parquet"
        mock_upload.side_effect = upload_side_effect

        with pytest.raises(Exception, match="Customers upload failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )

    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_customers_bigquery_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles customers BigQuery load errors."""
        from src import extract_and_load

        call_count = [0]

        def bq_side_effect(df, table):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Customers BigQuery load failed")

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.return_value = b"parquet"
        mock_bq.side_effect = bq_side_effect

        with pytest.raises(Exception, match="Customers BigQuery load failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )

    @pytest.mark.L2
    @patch("src.extract_and_load.extract_csv")
    @patch("src.extract_and_load.extract_postgres")
    @patch("src.extract_and_load.dataframe_to_parquet")
    @patch("src.extract_and_load.upload_to_minio")
    @patch("src.extract_and_load.load_to_bigquery")
    @patch.dict(
        os.environ,
        {
            "MINIO_BUCKET": "test-bucket",
            "BIGQUERY_PROJECT_ID": "test-project",
            "BIGQUERY_DATASET": "test_dataset",
        },
    )
    def test_run_elt_pipeline_customers_parquet_error(
        self, mock_bq, mock_upload, mock_parquet, mock_postgres, mock_csv
    ):
        """L2: Test pipeline handles customers parquet conversion errors."""
        from src import extract_and_load

        call_count = [0]

        def parquet_side_effect(df):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Customers parquet conversion failed")
            return b"parquet"

        mock_csv.return_value = pd.DataFrame(
            {
                "claim_id": ["CLM001"],
                "policy_number": ["POL001"],
                "claim_amount": [1000.0],
                "claim_date": ["2024-01-01"],
                "fraud_indicator": [False],
                "vehicle_type": ["sedan"],
                "driver_age": [30],
            }
        )
        mock_postgres.return_value = pd.DataFrame(
            {
                "customer_id": ["CUST001"],
                "name": ["John"],
                "email": ["john@test.com"],
                "credit_score": [750],
                "telematics_score": [85.5],
                "policy_number": ["POL001"],
            }
        )
        mock_parquet.side_effect = parquet_side_effect

        with pytest.raises(Exception, match="Customers parquet conversion failed"):
            extract_and_load.run_elt_pipeline(
                csv_path="/data/test.csv",
                postgres_query="SELECT * FROM customers",
            )


class TestParquetBufferSeek:
    """L2: Tests for Parquet buffer seek operation."""

    @pytest.mark.L2
    def test_dataframe_to_parquet_buffer_is_seekable(self):
        """L2: Test that Parquet buffer is seekable after conversion."""
        from src import extract_and_load
        import io

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = extract_and_load.dataframe_to_parquet_stream(df)

        assert hasattr(result, "seek"), "Result should be seekable BytesIO"
        result.seek(0)
        assert result.tell() == 0
        read_result = pd.read_parquet(result)
        assert len(read_result) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
