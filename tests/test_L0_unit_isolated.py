"""
================================================================================
Insurance Company Data Pipeline - L0 Unit Tests (Isolated)
================================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L0: Unit tests - Pure functions, no I/O, no external dependencies

Usage:
    pytest tests/test_L0_unit_isolated.py -v
================================================================================
"""

import pytest
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime


class TestDataGenerators:
    """L0: Test pure data generation functions."""

    @pytest.fixture
    def sample_customers_df(self):
        """Sample customers DataFrame."""
        return pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "first_name": ["John", "Jane", "Bob"],
                "last_name": ["Doe", "Smith", "Johnson"],
                "email": ["john@example.com", "jane@example.com", "bob@example.com"],
                "credit_score": [750, 680, 800],
                "annual_income": [75000.0, 50000.0, 120000.0],
            }
        )

    @pytest.fixture
    def sample_claims_df(self):
        """Sample claims DataFrame."""
        return pd.DataFrame(
            {
                "claim_id": [1, 2, 3],
                "customer_id": [1, 1, 2],
                "claim_amount": [5000.0, 7500.0, 3000.0],
                "claim_status": ["Open", "Closed", "Pending"],
                "claim_type": ["Auto", "Home", "Life"],
            }
        )

    def test_customers_df_created(self, sample_customers_df):
        """L0: Test customers DataFrame is created correctly."""
        assert len(sample_customers_df) == 3
        assert "customer_id" in sample_customers_df.columns
        assert "email" in sample_customers_df.columns

    def test_claims_df_created(self, sample_claims_df):
        """L0: Test claims DataFrame is created correctly."""
        assert len(sample_claims_df) == 3
        assert "claim_id" in sample_claims_df.columns
        assert "claim_amount" in sample_claims_df.columns

    def test_claims_df_status_values(self, sample_claims_df):
        """L0: Test claim status values are valid."""
        valid_statuses = {"Open", "Closed", "Pending", "Denied", "Investigation"}
        actual_statuses = set(sample_claims_df["claim_status"].unique())
        assert actual_statuses.issubset(valid_statuses)

    def test_claims_df_amount_positive(self, sample_claims_df):
        """L0: Test all claim amounts are positive."""
        assert (sample_claims_df["claim_amount"] > 0).all()

    def test_customer_income_positive(self, sample_customers_df):
        """L0: Test all incomes are positive."""
        assert (sample_customers_df["annual_income"] > 0).all()


class TestDataValidation:
    """L0: Test pure validation functions."""

    def test_validate_email_format(self):
        """L0: Test email validation."""

        def is_valid_email(email):
            if "@" not in email:
                return False
            parts = email.split("@")
            if len(parts) != 2:
                return False
            local, domain = parts
            if not local or not domain:
                return False
            if "." not in domain:
                return False
            return True

        assert is_valid_email("john@example.com") == True
        assert is_valid_email("jane.doe@company.org") == True
        assert is_valid_email("not-an-email") == False
        assert is_valid_email("@example.com") == False
        assert is_valid_email("noat") == False

    def test_validate_credit_score_range(self):
        """L0: Test credit score is in valid range."""
        scores = [500, 650, 750, 800, 850]
        for score in scores:
            assert 300 <= score <= 850

    def test_validate_claim_status(self):
        """L0: Test valid claim statuses."""
        valid_statuses = ["Open", "Closed", "Pending", "Denied", "Investigation"]
        test_claim = "Open"
        assert test_claim in valid_statuses

    def test_validate_date_range(self):
        """L0: Test date parsing and range."""
        date_str = "2024-01-15"
        date = datetime.strptime(date_str, "%Y-%m-%d")
        assert date.year >= 2020
        assert date.year <= 2030


class TestPipelineConfig:
    """L0: Test configuration functions."""

    def test_postgres_config_defaults(self):
        """L0: Test default PostgreSQL config."""
        config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "insurance_db"),
            "user": os.getenv("POSTGRES_USER", "insurance_user"),
            "password": os.getenv("POSTGRES_PASSWORD", "insurance_pass"),
        }
        assert config["host"] == "localhost"
        assert config["port"] == 5432

    def test_minio_config_defaults(self):
        """L0: Test default MinIO config."""
        config = {
            "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9900"),
            "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "bucket": os.getenv("MINIO_BUCKET", "insurance-data"),
        }
        assert config["endpoint"] == "localhost:9900"
        assert config["bucket"] == "insurance-data"

    def test_clickhouse_config_defaults(self):
        """L0: Test default ClickHouse config."""
        config = {
            "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
            "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
            "database": os.getenv("CLICKHOUSE_DB", "insurance_db"),
            "user": os.getenv("CLICKHOUSE_USER", "default"),
            "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass"),
        }
        assert config["host"] == "localhost"
        assert config["port"] == 8123


class TestDataTransformations:
    """L0: Test pure transformation functions."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [100, 200, 300],
                "category": ["A", "B", "A"],
            }
        )

    def test_add_risk_bucket(self, sample_df):
        """L0: Test risk bucket calculation."""

        def calculate_risk_bucket(credit_score):
            if credit_score >= 750:
                return "Low"
            elif credit_score >= 650:
                return "Medium"
            return "High"

        df = sample_df.copy()
        df["risk_bucket"] = df["value"].apply(calculate_risk_bucket)
        assert "risk_bucket" in df.columns

    def test_aggregate_by_status(self, sample_df):
        """L0: Test aggregation by category."""
        result = sample_df.groupby("category")["value"].sum().to_dict()
        assert isinstance(result, dict)
        assert result.get("A", 0) == 400  # 100 + 300
        assert result.get("B", 0) == 200

    def test_filter_by_amount_threshold(self, sample_df):
        """L0: Test filtering by amount."""
        threshold = 150
        filtered = sample_df[sample_df["value"] >= threshold]
        assert len(filtered) == 2


class TestGoCodeCompilation:
    """L0: Test Go code structure (static analysis)."""

    def test_go_file_exists(self):
        """L0: Test Go file exists."""
        assert Path("synthetic_data_generator.go").exists()

    def test_go_mod_exists(self):
        """L0: Test go.mod exists."""
        assert Path("go.mod").exists()

    def test_reset_database_go_exists(self):
        """L0: Test reset script exists."""
        assert Path("scripts/reset_database.go").exists()

    def test_go_code_has_main_function(self):
        """L0: Test Go code has main function."""
        content = Path("synthetic_data_generator.go").read_text()
        assert "func main()" in content

    def test_go_code_has_postgres_import(self):
        """L0: Test Go code imports postgres driver."""
        content = Path("synthetic_data_generator.go").read_text()
        assert "lib/pq" in content


class TestDBTModels:
    """L0: Test DBT model files (static analysis)."""

    def test_dbt_project_yml_exists(self):
        """L0: Test dbt_project.yml exists."""
        assert Path("dbt/dbt_project.yml").exists()

    def test_dbt_profiles_yml_exists(self):
        """L0: Test profiles.yml exists."""
        assert Path("dbt/profiles.yml").exists()

    def test_silver_models_exist(self):
        """L0: Test silver models exist."""
        silver_dir = Path("dbt/models/silver")
        assert silver_dir.exists()
        assert list(silver_dir.glob("*.sql"))

    def test_gold_models_exist(self):
        """L0: Test gold models exist."""
        gold_dir = Path("dbt/models/gold")
        assert gold_dir.exists()
        assert len(list(gold_dir.glob("*.sql"))) >= 3


class TestPythonScripts:
    """L0: Test Python script structure."""

    def test_pipeline_py_exists(self):
        """L0: Test pipeline.py exists."""
        assert Path("pipeline.py").exists()

    def test_dlt_pipeline_exists(self):
        """L0: Test DLT pipeline exists."""
        assert Path("scripts/dlt_pipeline.py").exists()

    def test_docker_compose_exists(self):
        """L0: Test docker-compose.yml exists."""
        assert Path("docker-compose.yml").exists()


class TestDockerCompose:
    """L0: Test docker-compose configuration."""

    def test_docker_compose_has_postgres(self):
        """L0: Test docker-compose has PostgreSQL."""
        content = Path("docker-compose.yml").read_text()
        assert "postgres:" in content

    def test_docker_compose_has_minio(self):
        """L0: Test docker-compose has MinIO."""
        content = Path("docker-compose.yml").read_text()
        assert "minio:" in content

    def test_docker_compose_has_clickhouse(self):
        """L0: Test docker-compose has ClickHouse."""
        content = Path("docker-compose.yml").read_text()
        assert "clickhouse:" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
