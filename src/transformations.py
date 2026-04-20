"""
Insurance Data Transformations
==============================

Actual transformation functions used by the pipeline.
These are imported and tested in L0 tests.
"""

import pandas as pd
from typing import Optional


# Risk Bucket Constants
RISK_BUCKET_EXCELLENT = "Excellent"
RISK_BUCKET_GOOD = "Good"
RISK_BUCKET_FAIR = "Fair"
RISK_BUCKET_POOR = "Poor"
RISK_BUCKET_UNKNOWN = "Unknown"

# Claim Status Category Constants
CLAIM_STATUS_OPEN = "Open"
CLAIM_STATUS_CLOSED = "Closed"
CLAIM_STATUS_DENIED = "Denied"
CLAIM_STATUS_OTHER = "Other"

# Vehicle Category Constants
VEHICLE_CATEGORY_CAR = "Car"
VEHICLE_CATEGORY_SUV = "SUV"
VEHICLE_CATEGORY_TRUCK = "Truck"
VEHICLE_CATEGORY_MOTORCYCLE = "Motorcycle"
VEHICLE_CATEGORY_OTHER = "Other"


def calculate_risk_bucket(credit_score: Optional[int]) -> str:
    """
    Calculate risk bucket from credit score.
    - Excellent: credit_score >= 750
    - Good:      credit_score >= 700
    - Fair:     credit_score >= 650
    - Poor:     credit_score < 650
    """
    if credit_score is None or pd.isna(credit_score):
        return RISK_BUCKET_UNKNOWN
    
    if credit_score >= 750:
        return RISK_BUCKET_EXCELLENT
    if credit_score >= 700:
        return RISK_BUCKET_GOOD
    if credit_score >= 650:
        return RISK_BUCKET_FAIR
    return RISK_BUCKET_POOR


def calculate_claim_status_category(status: str) -> str:
    """
    Calculate claim status category.
    - Closed:  claim_status = 'Closed'
    - Denied:  claim_status = 'Denied'
    - Open:    claim_status = 'Open'
    - Other:   All other statuses
    """
    if status in [CLAIM_STATUS_OPEN, CLAIM_STATUS_CLOSED, CLAIM_STATUS_DENIED]:
        return status
    return CLAIM_STATUS_OTHER


def calculate_vehicle_category(vehicle_type: str) -> str:
    """
    Calculate vehicle category.
    - Car:        Sedan, Coupe, Wagon
    - SUV:         SUV
    - Truck:       Truck
    - Motorcycle:  Motorcycle
    - Other:       All other types
    """
    if vehicle_type in ["Sedan", "Coupe", "Wagon"]:
        return VEHICLE_CATEGORY_CAR
    if vehicle_type == "SUV":
        return VEHICLE_CATEGORY_SUV
    if vehicle_type == "Truck":
        return VEHICLE_CATEGORY_TRUCK
    if vehicle_type == "Motorcycle":
        return VEHICLE_CATEGORY_MOTORCYCLE
    return VEHICLE_CATEGORY_OTHER


def apply_risk_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """Apply risk bucket calculation to DataFrame."""
    df = df.copy()
    if "credit_score" in df.columns:
        df["risk_bucket"] = df["credit_score"].apply(calculate_risk_bucket)
    return df


def apply_claim_status_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Apply claim status category to DataFrame."""
    df = df.copy()
    if "claim_status" in df.columns:
        df["claim_status_category"] = df["claim_status"].apply(
            calculate_claim_status_category
        )
    return df


def apply_vehicle_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Apply vehicle category to DataFrame."""
    df = df.copy()
    if "vehicle_type" in df.columns:
        df["vehicle_category"] = df["vehicle_type"].apply(calculate_vehicle_category)
    return df


class Config:
    """Application configuration."""

    POSTGRES_HOST = "localhost"
    POSTGRES_PORT = 5435
    POSTGRES_DB = "insurance_db"
    POSTGRES_USER = "insurance_user"
    POSTGRES_PASSWORD = "insurance_pass"

    MINIO_ENDPOINT = "localhost:9900"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    MINIO_BUCKET = "insurance-data"

    CLICKHOUSE_HOST = "localhost"
    CLICKHOUSE_PORT = 8123
    CLICKHOUSE_DB = "insurance_db"
    CLICKHOUSE_USER = "default"
    CLICKHOUSE_PASSWORD = "clickhouse_pass"


def validate_credit_score(score: int) -> bool:
    """Validate credit score is in range 300-850."""
    if score is None or pd.isna(score):
        return False
    return 300 <= score <= 850


def validate_claim_amount(amount: float) -> bool:
    """Validate claim amount is positive."""
    if amount is None or pd.isna(amount):
        return False
    return amount > 0 and amount <= 10_000_000
