#!/usr/bin/env python3
"""
Download insurance claims data from Kaggle.
Usage: python scripts/download_kaggle_data.py
Requires: pip install kaggle
Setup: Put kaggle.json in ~/.kaggle/ or set KAGGLE_USERNAME/KAGGLE_KEY env vars
"""

import os
import zipfile
import pandas as pd
from pathlib import Path


KAGGLE_DATASET = "buntystas/vehicle-claims-data"  # Insurance claims dataset


def download_from_kaggle():
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("ERROR: kaggle package not installed")
        print("Run: pip install kaggle")
        return None

    # Authenticate
    api = KaggleApi()
    api.authenticate()

    # Download dataset
    print(f"Downloading: {KAGGLE_DATASET}")
    api.dataset_download_files(KAGGLE_DATASET, path="data/", unzip=True, quiet=False)

    # Find the CSV
    data_dir = Path("data")
    csv_files = list(data_dir.glob("*.csv"))

    if csv_files:
        csv_path = csv_files[0]
        print(f"\nDownloaded: {csv_path}")

        # Load and preview
        df = pd.read_csv(csv_path)
        print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        print(f"Columns: {list(df.columns)}")

        return csv_path

    return None


def generate_insurance_csv():
    """Generate realistic insurance claims CSV if Kaggle fails."""
    import random
    from datetime import datetime, timedelta

    print("\nGenerating realistic insurance claims data instead...")

    # Generate customer data
    customers = []
    first_names = [
        "John",
        "Jane",
        "Michael",
        "Sarah",
        "David",
        "Emily",
        "Robert",
        "Lisa",
        "James",
        "Mary",
        "William",
        "Patricia",
        "Richard",
        "Jennifer",
        "Thomas",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
        "Wilson",
        "Anderson",
    ]

    for i in range(1, 1001):
        first = random.choice(first_names)
        last = random.choice(last_names)
        customers.append(
            {
                "customer_id": f"CUST{i:06d}",
                "name": f"{first} {last}",
                "email": f"{first.lower()}.{last.lower()}{i}@email.com",
                "credit_score": random.randint(500, 850),
                "telematics_score": random.randint(0, 100),
                "policy_number": f"POL{i:06d}",
            }
        )

    customers_df = pd.DataFrame(customers)
    customers_df.to_csv("data/customer_profiles.csv", index=False)
    print(f"Created: data/customer_profiles.csv ({len(customers_df)} rows)")

    # Generate claims data
    vehicle_types = ["Sedan", "SUV", "Truck", "Coupe", "Hatchback", "Van", "Wagon"]
    claim_types = [
        "Collision",
        "Comprehensive",
        "Liability",
        "Property Damage",
        "Personal Injury",
    ]
    statuses = ["Approved", "Pending", "Rejected", "Under Review"]
    cities = [
        "Los Angeles",
        "Houston",
        "Phoenix",
        "Philadelphia",
        "San Antonio",
        "Dallas",
    ]
    accident_types = [
        "Rear-end",
        "Side collision",
        "Single vehicle",
        "Head-on",
        "Parking lot",
    ]

    claims = []
    base_date = datetime(2023, 1, 1)

    for i in range(1, 5001):
        policy_num = f"POL{random.randint(1, 1000):06d}"
        claim_date = base_date + timedelta(days=random.randint(0, 730))
        claim_type = random.choice(claim_types)

        # Amount based on type
        if claim_type == "Collision":
            amount = round(random.uniform(2000, 50000), 2)
        elif claim_type == "Comprehensive":
            amount = round(random.uniform(500, 15000), 2)
        else:
            amount = round(random.uniform(1000, 30000), 2)

        claims.append(
            {
                "claim_id": f"CLM{i:08d}",
                "policy_number": policy_num,
                "claim_date": claim_date.strftime("%Y-%m-%d"),
                "claim_amount": amount,
                "claim_type": claim_type,
                "claim_status": random.choice(statuses),
                "vehicle_type": random.choice(vehicle_types),
                "driver_age": random.randint(18, 75),
                "fraud_indicator": random.choice(
                    ["Y", "N", "N", "N", "N", "N", "N", "N"]
                ),
                "deductible": random.choice([250, 500, 1000, 1500]),
                "city": random.choice(cities),
                "accident_type": random.choice(accident_types),
            }
        )

    claims_df = pd.DataFrame(claims)
    claims_df.to_csv("data/vehicle_insurance_claims.csv", index=False)
    print(f"Created: data/vehicle_insurance_claims.csv ({len(claims_df)} rows)")

    # Also save to data/kaggle/ to match original pipeline path
    os.makedirs("data/kaggle", exist_ok=True)
    claims_df.to_csv("data/kaggle/vehicle_insurance_claims.csv", index=False)
    print(f"Created: data/kaggle/vehicle_insurance_claims.csv")

    return "data/kaggle/vehicle_insurance_claims.csv"


def main():
    # Create data directory
    os.makedirs("data", exist_ok=True)

    print("=" * 60)
    print("Insurance Claims Data Downloader")
    print("=" * 60)

    # Try Kaggle first, fallback to generation
    try:
        csv_path = download_from_kaggle()
    except Exception as e:
        print(f"Kaggle download failed: {e}")
        csv_path = None

    if csv_path is None:
        csv_path = generate_insurance_csv()

    print(f"\n✓ Data ready: {csv_path}")
    print("\nNow run the pipeline:")
    print("  python src/extract_and_load.py")

    # Also load directly to PostgreSQL
    print("\nLoading to PostgreSQL...")
    try:
        from sqlalchemy import create_engine, text
        import pandas as pd

        conn_string = (
            "postgresql://insurance_user:insurance_pass@localhost:5433/insurance_db"
        )
        engine = create_engine(conn_string)

        with engine.connect() as conn:
            # Create tables
            conn.execute(
                text("""
                DROP TABLE IF EXISTS customer_profiles CASCADE;
                CREATE TABLE customer_profiles (
                    customer_id VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    credit_score INTEGER,
                    telematics_score INTEGER,
                    policy_number VARCHAR(20)
                )
            """)
            )
            conn.execute(
                text("""
                DROP TABLE IF EXISTS claims CASCADE;
                CREATE TABLE claims (
                    claim_id VARCHAR(20) PRIMARY KEY,
                    policy_number VARCHAR(20),
                    claim_date DATE,
                    claim_amount DECIMAL(12,2),
                    claim_type VARCHAR(50),
                    claim_status VARCHAR(20),
                    vehicle_type VARCHAR(20),
                    driver_age INTEGER,
                    fraud_indicator CHAR(1),
                    deductible INTEGER,
                    city VARCHAR(50),
                    accident_type VARCHAR(50),
                    total_paid DECIMAL(12,2) DEFAULT 0,
                    days_to_settle INTEGER DEFAULT 30,
                    repair_cost DECIMAL(12,2) DEFAULT 0
                )
            """)
            )
            conn.commit()

            # Create Tableau view for Insurance Claims accelerator
            conn.execute(
                text("""
                CREATE OR REPLACE VIEW insurance_claims_extract AS
                SELECT 
                    city AS "Agent Group",
                    'Default Agent' AS "Agent",
                    claim_type AS "Business Line",
                    claim_id AS "Claim Number",
                    COALESCE(total_paid, 0) AS "Claim Paid Amount",
                    accident_type AS "Claim Reason",
                    claim_status AS "Claim Status",
                    (claim_date + INTERVAL '1 day' * COALESCE(days_to_settle, 30)) AS "Close Date",
                    COALESCE(repair_cost, 0) AS "Damages Amount",
                    claim_date AS "Event Date",
                    CASE WHEN claim_status IN ('Approved', 'Paid') THEN 'Yes' ELSE 'No' END AS "Is Closed Flag",
                    CASE WHEN claim_status = 'Paid' THEN 'Yes' ELSE 'No' END AS "Is Reimbursed Flag",
                    claim_date AS "Open Date",
                    policy_number AS "Policy Holder",
                    policy_number AS "Policy Number",
                    vehicle_type AS "Policy Type"
                FROM claims
            """)
            )
            conn.commit()

        # Load CSV data to PostgreSQL
        claims_df = pd.read_csv("data/kaggle/vehicle_insurance_claims.csv")
        claims_df.to_sql("claims", engine, if_exists="replace", index=False)

        # Load customer data if exists
        if os.path.exists("data/customer_profiles.csv"):
            customers_df = pd.read_csv("data/customer_profiles.csv")
            customers_df.to_sql(
                "customer_profiles", engine, if_exists="replace", index=False
            )
            print(f"Loaded {len(customers_df)} customers to PostgreSQL")

        print(f"Loaded {len(claims_df)} claims to PostgreSQL")
        engine.dispose()
        print("\n✓ PostgreSQL updated!")

    except Exception as e:
        print(f"PostgreSQL load skipped: {e}")
        print("Make sure PostgreSQL is running first.")


if __name__ == "__main__":
    main()
