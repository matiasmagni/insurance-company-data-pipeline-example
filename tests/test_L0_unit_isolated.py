"""
============================================================================
Insurance Company Data Pipeline - L0 Unit Tests (Isolated)
============================================================================
Copyright (c) 2026 BugMentor (https://bugmentor.com)

L0: Pure unit tests - NO file checks, NO configs, ONLY business logic
     Test actual transformation/validation rules that drive the analytics

Usage:
    pytest tests/test_L0_unit_isolated.py -v
============================================================================
"""

import pytest
import pandas as pd


# =============================================================================
# CORE BUSINESS RULE: Risk Bucket (from DBT silver_customers)
# =============================================================================
# DBT SQL: CASE WHEN credit_score >= 750 THEN 'Excellent' WHEN credit_score >= 700
#              THEN 'Good' WHEN credit_score >= 650 THEN 'Fair' ELSE 'Poor' END


class TestRiskBucketRule:
    """L0: Test risk bucket business rule."""

    def credit_to_risk(self, credit_score: int) -> str:
        """Pure function: credit score to risk bucket."""
        if credit_score >= 750:
            return "Excellent"
        elif credit_score >= 700:
            return "Good"
        elif credit_score >= 650:
            return "Fair"
        else:
            return "Poor"

    def test_excellent_when_credit_750_plus(self):
        assert self.credit_to_risk(750) == "Excellent"
        assert self.credit_to_risk(800) == "Excellent"

    def test_good_when_credit_700_to_749(self):
        assert self.credit_to_risk(700) == "Good"
        assert self.credit_to_risk(725) == "Good"

    def test_fair_when_credit_650_to_699(self):
        assert self.credit_to_risk(650) == "Fair"
        assert self.credit_to_risk(675) == "Fair"

    def test_poor_when_credit_below_650(self):
        assert self.credit_to_risk(300) == "Poor"
        assert self.credit_to_risk(649) == "Poor"


class TestRiskBucketDistribution:
    """L0: Test risk distribution on sample data."""

    def test_distribution_percentages(self):
        """L0: Verify distribution math."""
        scores = [800, 750, 725, 700, 650, 600, 500]
        risk_map = {
            score: (
                "Excellent"
                if score >= 750
                else "Good"
                if score >= 700
                else "Fair"
                if score >= 650
                else "Poor"
            )
            for score in scores
        }

        counts = {}
        for r in risk_map.values():
            counts[r] = counts.get(r, 0) + 1

        assert counts["Excellent"] == 2
        assert counts["Good"] == 2
        assert counts["Fair"] == 1
        assert counts["Poor"] == 2


# =============================================================================
# CORE BUSINESS RULE: Claim Status Category (from DBT silver_claims)
# =============================================================================
# DBT SQL: CASE WHEN status='Closed' THEN 'Closed' WHEN status='Denied' THEN 'Denied'
#              WHEN status='Open' THEN 'Open' ELSE 'Other' END


class TestClaimStatusRule:
    """L0: Test claim status categorization."""

    def status_to_category(self, status: str) -> str:
        if status == "Closed":
            return "Closed"
        elif status == "Denied":
            return "Denied"
        elif status == "Open":
            return "Open"
        else:
            return "Other"

    def test_pending_maps_to_other(self):
        """L0: 'Pending' is not a valid category in silver."""
        assert self.status_to_category("Pending") == "Other"

    def test_investigation_maps_to_other(self):
        assert self.status_to_category("Investigation") == "Other"

    def test_closed_stays_closed(self):
        assert self.status_to_category("Closed") == "Closed"


# =============================================================================
# CORE BUSINESS RULE: Vehicle Category (from DBT silver_claims)
# =============================================================================
# DBT SQL: CASE WHEN vehicle IN('Sedan','Coupe','Wagon') THEN 'Car'
#              WHEN vehicle='SUV' THEN 'SUV' WHEN vehicle='Truck' THEN 'Truck'
#              WHEN vehicle='Motorcycle' THEN 'Motorcycle' ELSE 'Other' END


class TestVehicleCategoryRule:
    """L0: Test vehicle categorization."""

    def vehicle_to_category(self, vehicle: str) -> str:
        if vehicle in ("Sedan", "Coupe", "Wagon"):
            return "Car"
        elif vehicle == "SUV":
            return "SUV"
        elif vehicle == "Truck":
            return "Truck"
        elif vehicle == "Motorcycle":
            return "Motorcycle"
        else:
            return "Other"

    def test_sedan_is_car(self):
        assert self.vehicle_to_category("Sedan") == "Car"

    def test_suv_is_suv(self):
        assert self.vehicle_to_category("SUV") == "SUV"

    def test_van_is_other(self):
        """L0: Van is not a category in silver layer."""
        assert self.vehicle_to_category("Van") == "Other"


# =============================================================================
# CORE BUSINESS RULE: Claim Amount Validation
# =============================================================================


class TestClaimAmountValidation:
    """L0: Test claim amount business rules."""

    def is_valid_amount(self, amount: float) -> bool:
        """Business rule: claim amount > 0 and <= 10M."""
        return amount > 0 and amount <= 10_000_000

    def test_zero_is_invalid(self):
        assert self.is_valid_amount(0) is False

    def test_negative_is_invalid(self):
        assert self.is_valid_amount(-100) is False

    def test_over_10m_is_invalid(self):
        assert self.is_valid_amount(10_000_001) is False

    def test_5m_is_valid(self):
        assert self.is_valid_amount(5_000_000) is True


# =============================================================================
# CORE BUSINESS RULE: Credit Score Range
# =============================================================================


class TestCreditScoreValidation:
    """L0: Test credit score business rules."""

    def is_valid_score(self, score: int) -> bool:
        """Business rule: 300 <= credit_score <= 850."""
        return 300 <= score <= 850

    def test_300_is_min_valid(self):
        assert self.is_valid_score(300) is True

    def test_850_is_max_valid(self):
        assert self.is_valid_score(850) is True

    def test_below_300_is_invalid(self):
        assert self.is_valid_score(299) is False

    def test_above_850_is_invalid(self):
        assert self.is_valid_score(851) is False


# =============================================================================
# CORE BUSINESS RULE: Email Format
# =============================================================================


class TestEmailValidation:
    """L0: Test email validation business rule."""

    def is_valid_email(self, email: str) -> bool:
        if not email or "@" not in email:
            return False
        parts = email.split("@")
        return len(parts) == 2 and all(parts)

    def test_valid_email(self):
        assert self.is_valid_email("test@example.com") is True

    def test_no_at_sign(self):
        assert self.is_valid_email("test.example.com") is False


# =============================================================================
# CORE BUSINESS RULE: Aggregation Results
# =============================================================================


class TestAggregationMath:
    """L0: Test aggregation calculations."""

    def test_sum_by_category(self):
        """L0: Total claims by status."""
        df = pd.DataFrame(
            {
                "status": ["Open", "Open", "Closed", "Closed", "Denied"],
                "amount": [1000, 2000, 3000, 1500, 500],
            }
        )
        result = df.groupby("status")["amount"].sum()

        assert result["Open"] == 3000
        assert result["Closed"] == 4500
        assert result["Denied"] == 500

    def test_avg_by_category(self):
        """L0: Average claim by status."""
        df = pd.DataFrame({"status": ["Open", "Closed"], "amount": [1000, 2000]})
        result = df.groupby("status")["amount"].mean()

        assert result["Open"] == 1000
        assert result["Closed"] == 2000


# =============================================================================
# CORE BUSINESS RULE: Data Completeness Check
# =============================================================================


class TestDataCompleteness:
    """L0: Test data completeness rules."""

    def test_has_null_detection(self):
        """L0: Can detect null values in DataFrame."""
        df = pd.DataFrame(
            {"customer_id": [1, 2, None, 4], "name": ["A", "B", "C", "D"]}
        )
        has_null = bool(df["customer_id"].isna().any())
        assert has_null is True

    def test_null_count(self):
        """L0: Count null values."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [100, None, 300]})
        null_count = int(df["value"].isna().sum())
        assert null_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
