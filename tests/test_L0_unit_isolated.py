"""
============================================================================
Insurance Company Data Pipeline - L0 Unit Tests (Isolated)
============================================================================

L0: Test ACTUAL functions from src/transformations.py - No I/O, No mocks

Usage:
    pytest tests/test_L0_unit_isolated.py -v
============================================================================
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transformations import (
    calculate_risk_bucket,
    calculate_claim_status_category,
    calculate_vehicle_category,
    apply_risk_buckets,
    apply_claim_status_categories,
    apply_vehicle_categories,
    validate_credit_score,
    validate_claim_amount,
    Config,
)


class TestCalculateRiskBucket:
    """L0: Test actual calculate_risk_bucket function."""

    def test_excellent_credit_750_plus(self):
        assert calculate_risk_bucket(750) == "Excellent"
        assert calculate_risk_bucket(800) == "Excellent"

    def test_good_credit_700_to_749(self):
        assert calculate_risk_bucket(700) == "Good"
        assert calculate_risk_bucket(725) == "Good"

    def test_fair_credit_650_to_699(self):
        assert calculate_risk_bucket(650) == "Fair"
        assert calculate_risk_bucket(699) == "Fair"

    def test_poor_credit_below_650(self):
        assert calculate_risk_bucket(500) == "Poor"
        assert calculate_risk_bucket(649) == "Poor"

    def test_none_returns_unknown(self):
        assert calculate_risk_bucket(None) == "Unknown"


class TestCalculateClaimStatusCategory:
    """L0: Test actual calculate_claim_status_category function."""

    def test_open_stays_open(self):
        assert calculate_claim_status_category("Open") == "Open"

    def test_closed_stays_closed(self):
        assert calculate_claim_status_category("Closed") == "Closed"

    def test_denied_stays_denied(self):
        assert calculate_claim_status_category("Denied") == "Denied"

    def test_pending_becomes_other(self):
        assert calculate_claim_status_category("Pending") == "Other"


class TestCalculateVehicleCategory:
    """L0: Test actual calculate_vehicle_category function."""

    def test_sedan_is_car(self):
        assert calculate_vehicle_category("Sedan") == "Car"

    def test_coupe_is_car(self):
        assert calculate_vehicle_category("Coupe") == "Car"

    def test_suv_is_suv(self):
        assert calculate_vehicle_category("SUV") == "SUV"

    def test_truck_is_truck(self):
        assert calculate_vehicle_category("Truck") == "Truck"

    def test_motorcycle_is_motorcycle(self):
        assert calculate_vehicle_category("Motorcycle") == "Motorcycle"

    def test_van_is_other(self):
        assert calculate_vehicle_category("Van") == "Other"


class TestApplyRiskBuckets:
    """L0: Test actual apply_risk_buckets function."""

    def test_applies_to_dataframe(self):
        df = pd.DataFrame({"customer_id": [1, 2, 3], "credit_score": [800, 650, 500]})

        result = apply_risk_buckets(df)

        assert "risk_bucket" in result.columns
        assert result.loc[0, "risk_bucket"] == "Excellent"
        assert result.loc[1, "risk_bucket"] == "Fair"
        assert result.loc[2, "risk_bucket"] == "Poor"


class TestApplyClaimStatusCategories:
    """L0: Test actual apply_claim_status_categories function."""

    def test_applies_to_dataframe(self):
        df = pd.DataFrame(
            {"claim_id": [1, 2, 3], "claim_status": ["Open", "Closed", "Pending"]}
        )

        result = apply_claim_status_categories(df)

        assert "claim_status_category" in result.columns
        assert result.loc[0, "claim_status_category"] == "Open"
        assert result.loc[2, "claim_status_category"] == "Other"


class TestApplyVehicleCategories:
    """L0: Test actual apply_vehicle_categories function."""

    def test_applies_to_dataframe(self):
        df = pd.DataFrame(
            {"claim_id": [1, 2, 3], "vehicle_type": ["Sedan", "SUV", "Van"]}
        )

        result = apply_vehicle_categories(df)

        assert "vehicle_category" in result.columns
        assert result.loc[0, "vehicle_category"] == "Car"
        assert result.loc[1, "vehicle_category"] == "SUV"


class TestValidateCreditScore:
    """L0: Test actual validate_credit_score function."""

    def test_valid_scores(self):
        assert validate_credit_score(300) is True
        assert validate_credit_score(500) is True
        assert validate_credit_score(850) is True

    def test_invalid_scores(self):
        assert validate_credit_score(299) is False
        assert validate_credit_score(851) is False
        assert validate_credit_score(None) is False


class TestValidateClaimAmount:
    """L0: Test actual validate_claim_amount function."""

    def test_valid_amounts(self):
        assert validate_claim_amount(0.01) is True
        assert validate_claim_amount(1000.0) is True

    def test_invalid_amounts(self):
        assert validate_claim_amount(0) is False
        assert validate_claim_amount(-100) is False
        assert validate_claim_amount(10_000_001) is False


class TestConfig:
    """L0: Test actual Config class."""

    def test_postgres_defaults(self):
        assert Config.POSTGRES_HOST == "localhost"
        assert Config.POSTGRES_PORT == 5432

    def test_minio_defaults(self):
        assert Config.MINIO_ENDPOINT == "localhost:9900"
        assert Config.MINIO_BUCKET == "insurance-data"

    def test_clickhouse_defaults(self):
        assert Config.CLICKHOUSE_HOST == "localhost"
        assert Config.CLICKHOUSE_PORT == 8123


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
