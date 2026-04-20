#!/usr/bin/env python3
"""
Download/Generate insurance claims data.
Usage: python scripts/download_kaggle_data.py
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


def generate_insurance_csv():
    """Generate realistic insurance claims CSV files."""
    print("\nGenerating realistic insurance claims data...")
    DATA_DIR.mkdir(exist_ok=True)

    # Generate customer data
    customers = []
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]

    for i in range(1, 1001):
        first = random.choice(first_names)
        last = random.choice(last_names)
        customers.append({
            "customer_id": f"CUST{i:06d}",
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@email.com",
            "credit_score": random.randint(500, 850),
            "telematics_score": random.randint(0, 100),
            "policy_number": f"POL{i:06d}"
        })

    customers_df = pd.DataFrame(customers)
    customers_df.to_csv(DATA_DIR / "customer_profiles.csv", index=False)
    print(f"Created: {DATA_DIR / 'customer_profiles.csv'} ({len(customers_df)} rows)")

    # Generate claims data
    vehicle_types = ["Sedan", "SUV", "Truck", "Coupe", "Hatchback", "Van", "Wagon"]
    claim_types = ["Collision", "Comprehensive", "Liability", "Property Damage"]
    statuses = ["Approved", "Pending", "Rejected", "Under Review"]
    
    claims = []
    base_date = datetime(2023, 1, 1)

    for i in range(1, 2001):
        policy_num = f"POL{random.randint(1, 1000):06d}"
        claim_date = base_date + timedelta(days=random.randint(0, 365))
        
        claims.append({
            "claim_id": f"CLM{i:08d}",
            "policy_number": policy_num,
            "claim_date": claim_date.strftime("%Y-%m-%d"),
            "claim_amount": round(random.uniform(500, 25000), 2),
            "claim_type": random.choice(claim_types),
            "claim_status": random.choice(statuses),
            "vehicle_type": random.choice(vehicle_types),
            "driver_age": random.randint(18, 75),
            "fraud_indicator": random.choice(["Y", "N", "N", "N"]),
            "city": random.choice(["Los Angeles", "New York", "Chicago"]),
        })

    claims_df = pd.DataFrame(claims)
    claims_df.to_csv(DATA_DIR / "vehicle_insurance_claims.csv", index=False)
    print(f"Created: {DATA_DIR / 'vehicle_insurance_claims.csv'} ({len(claims_df)} rows)")


def main():
    print("=" * 60)
    print("Insurance Data Generator")
    print("=" * 60)
    
    generate_insurance_csv()
    
    print("\n✓ Data ready in data/ directory!")


if __name__ == "__main__":
    main()
