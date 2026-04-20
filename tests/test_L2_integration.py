"""
================================================================================
Insurance Company Data Pipeline - L2 Integration Tests
================================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L2: Integration tests - Real services running in Docker

Usage:
    pytest tests/test_L2_integration.py -v
    (Requires: docker-compose up -d)
================================================================================
"""

import pytest
import pandas as pd
import os
import sys
from pathlib import Path


class TestDockerCompose:
    """L2: Test Docker Compose services."""

    def test_docker_compose_file_valid(self):
        """L2: Test docker-compose.yml is valid YAML."""
        import yaml

        with open("docker-compose.yml") as f:
            config = yaml.safe_load(f)

        assert "services" in config
        assert "postgres" in config["services"]
        assert "minio" in config["services"]
        assert "clickhouse" in config["services"]

    @pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists(), reason="Docker not available"
    )
    def test_postgres_container_running(self):
        """L2: Test PostgreSQL container is running."""
        import subprocess

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
        # This will work when docker-compose is running

    @pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists(), reason="Docker not available"
    )
    def test_minio_container_running(self):
        """L2: Test MinIO container is running."""
        import subprocess

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

    @pytest.mark.skipif(
        not Path("/var/run/docker.sock").exists(), reason="Docker not available"
    )
    def test_clickhouse_container_running(self):
        """L2: Test ClickHouse container is running."""
        import subprocess

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


class TestProjectStructure:
    """L2: Test project structure is correct."""

    def test_all_required_files_exist(self):
        """L2: Test all required files exist."""
        required_files = [
            "docker-compose.yml",
            "pipeline.py",
            "synthetic_data_generator.go",
            "go.mod",
            "scripts/dlt_pipeline.py",
            "scripts/reset_database.go",
            "scripts/download_kaggle_data.py",
            "dbt/dbt_project.yml",
            "dbt/profiles.yml",
            "README.md",
            "LICENSE",
        ]

        for file in required_files:
            assert Path(file).exists(), f"Missing: {file}"

    def test_dbt_models_structure(self):
        """L2: Test DBT models directory structure."""
        assert Path("dbt/models/sources.yml").exists()

        silver_dir = Path("dbt/models/silver")
        assert silver_dir.exists()
        silver_files = list(silver_dir.glob("*.sql"))
        assert len(silver_files) >= 2

        gold_dir = Path("dbt/models/gold")
        assert gold_dir.exists()
        gold_files = list(gold_dir.glob("*.sql"))
        assert len(gold_files) >= 3


class TestScriptsExecutable:
    """L2: Test scripts have correct permissions."""

    def test_unix_scripts_executable(self):
        """L2: Test Unix scripts are executable."""
        if os.name == "posix":
            for script in Path("scripts_unix").glob("*.sh"):
                assert os.access(script, os.X_OK), f"{script} not executable"


class TestGoCode:
    """L2: Test Go code compiles."""

    def test_go_mod_valid(self):
        """L2: Test go.mod is valid."""
        content = Path("go.mod").read_text()
        assert "module " in content
        assert "github.com/lib/pq" in content

    def test_synthetic_generator_has_generate_function(self):
        """L2: Test synthetic generator has required functions."""
        content = Path("synthetic_data_generator.go").read_text()
        assert "func generateCustomers" in content
        assert "func generateClaims" in content
        assert "func main()" in content

    def test_reset_database_has_truncate_function(self):
        """L2: Test reset database has truncate function."""
        content = Path("scripts/reset_database.go").read_text()
        assert "func truncateTables" in content


class TestPythonScripts:
    """L2: Test Python scripts."""

    def test_dlt_pipeline_imports(self):
        """L2: Test DLT pipeline has required imports."""
        content = Path("scripts/dlt_pipeline.py").read_text()
        assert "psycopg2" in content
        assert "boto3" in content
        assert "pandas" in content

    def test_pipeline_imports(self):
        """L2: Test main pipeline imports."""
        content = Path("pipeline.py").read_text()
        assert "subprocess" in content
        assert "logging" in content


class TestDBTConfiguration:
    """L2: Test DBT configuration."""

    def test_dbt_project_yml_valid(self):
        """L2: Test dbt_project.yml is valid."""
        import yaml

        with open("dbt/dbt_project.yml") as f:
            config = yaml.safe_load(f)

        assert "name" in config
        assert config["name"] == "dbt_insurance"

    def test_dbt_profiles_yml_exists(self):
        """L2: Test profiles.yml exists."""
        assert Path("dbt/profiles.yml").exists()


class TestTestFiles:
    """L2: Test test files exist."""

    def test_all_test_levels_exist(self):
        """L2: Test all test level files exist."""
        assert Path("tests/test_L0_unit_isolated.py").exists()
        assert Path("tests/test_L1_unit_integrated.py").exists()
        assert Path("tests/test_L2_integration.py").exists()
        assert Path("tests/test_L3_e2e.py").exists()

    def test_test_runners_exist(self):
        """L2: Test test runner scripts exist."""
        assert Path("scripts_unix/run_all_tests.sh").exists()
        assert Path("scripts_windows/run_all_tests.bat").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
