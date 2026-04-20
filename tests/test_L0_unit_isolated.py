"""
============================================================================
Insurance Company Data Pipeline - L0 Unit Tests (Isolated)
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L0: Unit tests - Pure functions, no I/O, no external dependencies
     Test business logic, transformations, validations in isolation

Usage:
    pytest tests/test_L0_unit_isolated.py -v
============================================================================
"""

import pytest
import pandas as pd
from datetime import datetime


# =============================================================================
# Risk Bucket Calculation
# =============================================================================


class TestRiskBucketCalculation:
    """L0: Pure risk bucket calculation - no dependencies."""

    def calculate_risk_bucket(self, credit_score: int) -> str:
        if credit_score >= 750:
            return "Low"
        elif credit_score >= 650:
            return "Medium"
        return "High"

    def test_risk_bucket_low(self):
        assert self.calculate_risk_bucket(750) == "Low"
        assert self.calculate_risk_bucket(800) == "Low"
        assert self.calculate_risk_bucket(850) == "Low"

    def test_risk_bucket_medium(self):
        assert self.calculate_risk_bucket(650) == "Medium"
        assert self.calculate_risk_bucket(700) == "Medium"
        assert self.calculate_risk_bucket(749) == "Medium"

    def test_risk_bucket_high(self):
        assert self.calculate_risk_bucket(300) == "High"
        assert self.calculate_risk_bucket(500) == "High"
        assert self.calculate_risk_bucket(649) == "High"


# =============================================================================
# Claim Status Category
# =============================================================================


class TestClaimStatusCategory:
    """L0: Pure claim status categorization."""

    def categorize_claim_status(self, status: str) -> str:
        pending_statuses = {"Open", "Pending", "Investigation"}
        closed_statuses = {"Closed", "Denied"}

        if status in pending_statuses:
            return "Pending"
        elif status in closed_statuses:
            return "Closed"
        return "Unknown"

    def test_pending_category(self):
        for status in ["Open", "Pending", "Investigation"]:
            assert self.categorize_claim_status(status) == "Pending"

    def test_closed_category(self):
        for status in ["Closed", "Denied"]:
            assert self.categorize_claim_status(status) == "Closed"


# =============================================================================
# Vehicle Category
# =============================================================================


class TestVehicleCategory:
    """L0: Pure vehicle category assignment."""

    VEHICLE_CATEGORIES = {
        "Sedan": ["camry", "accord", "civic", "altima"],
        "SUV": ["explorer", "pilot", "highlander", "pathfinder"],
        "Truck": ["f-150", "silverado", "ram", "sierra"],
        "Luxury": ["bmw", "mercedes", "lexus", "audi"],
        "Sports": ["mustang", "camaro", "corvette", "challenger"],
    }

    def categorize_vehicle(self, vehicle_type: str) -> str:
        vehicle_type_lower = vehicle_type.lower()
        for category, models in self.VEHICLE_CATEGORIES.items():
            if any(model in vehicle_type_lower for model in models):
                return category
        return "Other"

    def test_sedan_category(self):
        assert self.categorize_vehicle("Toyota Camry") == "Sedan"
        assert self.categorize_vehicle("Honda Civic") == "Sedan"

    def test_suv_category(self):
        assert self.categorize_vehicle("Ford Explorer") == "SUV"
        assert self.categorize_vehicle("Honda Pilot") == "SUV"

    def test_truck_category(self):
        assert self.categorize_vehicle("Ford F-150") == "Truck"
        assert self.categorize_vehicle("Chevy Silverado") == "Truck"

    def test_luxury_category(self):
        assert self.categorize_vehicle("BMW 3 Series") == "Luxury"
        assert self.categorize_vehicle("Mercedes C-Class") == "Luxury"

    def test_other_category(self):
        assert self.categorize_vehicle("Unknown Vehicle") == "Other"


# =============================================================================
# Email Validation
# =============================================================================


class TestEmailValidation:
    """L0: Pure email validation."""

    def is_valid_email(self, email: str) -> bool:
        if not email or "@" not in email:
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

    def test_valid_emails(self):
        assert self.is_valid_email("john@example.com") is True
        assert self.is_valid_email("jane.doe@company.org") is True

    def test_invalid_emails(self):
        assert self.is_valid_email("not-an-email") is False
        assert self.is_valid_email("@example.com") is False


# =============================================================================
# Claim Amount Validation
# =============================================================================


class TestClaimAmountValidation:
    """L0: Pure claim amount validation."""

    def validate_claim_amount(self, amount: float) -> bool:
        return amount > 0 and amount <= 10_000_000

    def test_valid_amounts(self):
        assert self.validate_claim_amount(100.0) is True
        assert self.validate_claim_amount(5000.0) is True

    def test_invalid_amounts(self):
        assert self.validate_claim_amount(0) is False
        assert self.validate_claim_amount(-100.0) is False


# =============================================================================
# Data Frame Transformations
# =============================================================================


class TestDataFrameTransformations:
    """L0: Pure DataFrame transformations."""

    @pytest.fixture
    def sample_customers_df(self):
        return pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "credit_score": [750, 620, 800],
                "annual_income": [75000, 45000, 120000],
            }
        )

    @pytest.fixture
    def sample_claims_df(self):
        return pd.DataFrame(
            {
                "claim_id": [1, 2, 3, 4],
                "customer_id": [1, 1, 2, 3],
                "claim_amount": [5000, 2500, 10000, 7500],
                "claim_status": ["Open", "Closed", "Pending", "Denied"],
                "vehicle_type": [
                    "Toyota Camry",
                    "Ford F-150",
                    "Honda Pilot",
                    "BMW 3 Series",
                ],
            }
        )

    def test_add_risk_bucket_column(self, sample_customers_df):
        def calc_bucket(score):
            if score >= 750:
                return "Low"
            elif score >= 650:
                return "Medium"
            return "High"

        df = sample_customers_df.copy()
        df["risk_bucket"] = df["credit_score"].apply(calc_bucket)

        assert "risk_bucket" in df.columns

    def test_add_claim_status_category(self, sample_claims_df):
        def calc_status(status):
            if status in {"Open", "Pending", "Investigation"}:
                return "Pending"
            return "Closed"

        df = sample_claims_df.copy()
        df["claim_status_category"] = df["claim_status"].apply(calc_status)

        assert "claim_status_category" in df.columns

    def test_add_vehicle_category(self, sample_claims_df):
        vehicle_cats = {
            "Sedan": ["camry", "accord", "civic"],
            "SUV": ["explorer", "pilot", "highlander"],
            "Truck": ["f-150", "silverado", "ram"],
            "Luxury": ["bmw", "mercedes", "lexus"],
        }

        def calc_vehicle(vtype):
            vtype_lower = vtype.lower()
            for cat, models in vehicle_cats.items():
                if any(m in vtype_lower for m in models):
                    return cat
            return "Other"

        df = sample_claims_df.copy()
        df["vehicle_category"] = df["vehicle_type"].apply(calc_vehicle)

        assert "vehicle_category" in df.columns

    def test_aggregate_by_status(self, sample_claims_df):
        result = (
            sample_claims_df.groupby("claim_status")["claim_amount"].sum().to_dict()
        )
        assert result["Open"] == 5000

    def test_filter_by_amount_threshold(self, sample_claims_df):
        threshold = 5000
        filtered = sample_claims_df[sample_claims_df["claim_amount"] >= threshold]
        assert len(filtered) == 3


# =============================================================================
# Date Parsing
# =============================================================================


class TestDateParsing:
    """L0: Pure date parsing."""

    def parse_date(self, date_str: str) -> datetime:
        return datetime.strptime(date_str, "%Y-%m-%d")

    def test_valid_dates(self):
        date = self.parse_date("2024-01-15")
        assert date.year == 2024
        assert date.month == 1

    def test_date_in_valid_range(self):
        date = self.parse_date("2024-01-15")
        assert 2020 <= date.year <= 2030


# =============================================================================
# Aggregation Logic
# =============================================================================


class TestAggregationLogic:
    """L0: Pure aggregation logic."""

    def test_sum_by_agent(self):
        df = pd.DataFrame(
            {
                "agent_id": [1, 1, 2, 2, 2],
                "claim_amount": [1000, 2000, 3000, 1500, 500],
            }
        )
        result = df.groupby("agent_id")["claim_amount"].sum()
        assert result[1] == 3000
        assert result[2] == 5000

    def test_count_by_status(self):
        df = pd.DataFrame(
            {
                "claim_status": ["Closed", "Closed", "Open", "Closed", "Open"],
            }
        )
        result = df.groupby("claim_status").size()
        assert result["Closed"] == 3
        assert result["Open"] == 2

    def test_avg_by_business_line(self):
        df = pd.DataFrame(
            {
                "business_line": ["Auto", "Home", "Auto", "Home", "Life"],
                "claim_amount": [3000, 2000, 1000, 1500, 500],
            }
        )
        result = df.groupby("business_line")["claim_amount"].mean()
        assert result["Auto"] == 2000


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
