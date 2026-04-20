#!/usr/bin/env python3
"""
Generate realistic insurance claims data and customer profiles.
Loads data into PostgreSQL for the pipeline.
"""

import random
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text


def generate_customer_profiles(n: int = 1000) -> pd.DataFrame:
    """Generate realistic customer profiles."""
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

    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "email.com"]

    customers = []
    for i in range(1, n + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = f"{first.lower()}.{last.lower()}{i}@{random.choice(domains)}"

        customers.append(
            {
                "customer_id": f"CUST{i:06d}",
                "name": f"{first} {last}",
                "email": email,
                "credit_score": random.randint(500, 850),
                "telematics_score": random.randint(0, 100),
                "policy_number": f"POL{i:06d}",
                "state": random.choice(
                    ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI"]
                ),
                "annual_premium": round(random.uniform(500, 5000), 2),
                "years_customer": random.randint(1, 20),
            }
        )

    return pd.DataFrame(customers)


def generate_claims_data(n: int = 5000, policy_numbers: list = None) -> pd.DataFrame:
    """Generate realistic insurance claims data."""
    if policy_numbers is None:
        policy_numbers = [f"POL{i:06d}" for i in range(1, 1001)]

    vehicle_types = [
        "Sedan",
        "SUV",
        "Truck",
        "Coupe",
        "Hatchback",
        "Van",
        "Wagon",
        "Crossover",
    ]
    claim_types = [
        "Collision",
        "Comprehensive",
        "Liability",
        "Property Damage",
        "Personal Injury",
        "Uninsured Motorist",
        "Medical Payments",
        "Death",
    ]
    claim_statuses = ["Approved", "Pending", "Rejected", "Under Review", "Paid"]
    fraud_indicators = ["Y", "N", "N", "N", "N", "N", "N", "N"]  # ~12.5% fraud rate

    claims = []
    base_date = datetime(2023, 1, 1)

    for i in range(1, n + 1):
        claim_date = base_date + timedelta(days=random.randint(0, 730))

        # Claim amount varies by type
        claim_type = random.choice(claim_types)
        if claim_type == "Comprehensive":
            amount = round(random.uniform(500, 15000), 2)
        elif claim_type == "Collision":
            amount = round(random.uniform(1000, 50000), 2)
        elif claim_type == "Liability":
            amount = round(random.uniform(2000, 100000), 2)
        else:
            amount = round(random.uniform(200, 25000), 2)

        # Vehicle and driver info
        vehicle = random.choice(vehicle_types)
        driver_age = random.randint(18, 75)

        # Deductible based on premium
        deductible = random.choice([250, 500, 1000, 1500, 2000])

        # Days to settle
        days_to_settle = random.randint(1, 180)

        claims.append(
            {
                "claim_id": f"CLM{i:08d}",
                "policy_number": random.choice(policy_numbers),
                "claim_date": claim_date,
                "claim_amount": amount,
                "claim_type": claim_type,
                "claim_status": random.choice(claim_statuses),
                "vehicle_type": vehicle,
                "driver_age": driver_age,
                "fraud_indicator": random.choice(fraud_indicators),
                "deductible": deductible,
                "days_to_settle": days_to_settle,
                "repair_cost": round(amount * random.uniform(0.3, 0.9), 2)
                if amount > 1000
                else 0,
                "total_paid": round(amount * random.uniform(0.5, 1.0), 2)
                if random.random() > 0.2
                else 0,
                "city": random.choice(
                    [
                        "Los Angeles",
                        "Houston",
                        "Phoenix",
                        "Philadelphia",
                        "San Antonio",
                        "San Diego",
                        "Dallas",
                        "San Jose",
                        "Austin",
                        "Jacksonville",
                    ]
                ),
                "accident_type": random.choice(
                    [
                        "Rear-end",
                        "Side collision",
                        "Single vehicle",
                        "Head-on",
                        "Multi-vehicle",
                        "Parking lot",
                        "Hit and run",
                        "None",
                    ]
                ),
            }
        )

    return pd.DataFrame(claims)


def load_to_postgres(df: pd.DataFrame, table_name: str, engine):
    """Load DataFrame to PostgreSQL."""
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows to {table_name}")


def main():
    # PostgreSQL connection
    conn_string = (
        "postgresql://insurance_user:insurance_pass@localhost:5433/insurance_db"
    )
    engine = create_engine(conn_string)

    # Test connection
    try:
        with engine.connect() as conn:
            print("Connected to PostgreSQL successfully!")
    except Exception as e:
        print(f"ERROR: Could not connect to PostgreSQL: {e}")
        print("\nMake sure PostgreSQL is running. Try:")
        print("  docker run -d -e POSTGRES_PASSWORD=insurance_pass \\")
        print("    -e POSTGRES_USER=insurance_user \\")
        print("    -e POSTGRES_DB=insurance_db \\")
        print("    -p 5433:5432 postgres")
        return

    # Create tables manually to ensure correct schema
    with engine.connect() as conn:
        # Customer profiles table
        conn.execute(
            text("""
            DROP TABLE IF EXISTS customer_profiles CASCADE;
            CREATE TABLE customer_profiles (
                customer_id VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                credit_score INTEGER,
                telematics_score INTEGER,
                policy_number VARCHAR(20),
                state VARCHAR(2),
                annual_premium DECIMAL(10,2),
                years_customer INTEGER
            )
        """)
        )

        # Claims table
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
                days_to_settle INTEGER,
                repair_cost DECIMAL(12,2),
                total_paid DECIMAL(12,2),
                city VARCHAR(50),
                accident_type VARCHAR(50)
            )
        """)
        )
        conn.commit()

    # Generate data
    print("\nGenerating customer profiles...")
    customers_df = generate_customer_profiles(1000)

    print("Generating claims data...")
    policy_numbers = customers_df["policy_number"].tolist()
    claims_df = generate_claims_data(5000, policy_numbers)

    # Load to PostgreSQL
    print("\nLoading data to PostgreSQL...")
    load_to_postgres(customers_df, "customer_profiles", engine)
    load_to_postgres(claims_df, "claims", engine)

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM customer_profiles"))
        print(f"\nTotal customers in database: {result.scalar()}")

        result = conn.execute(text("SELECT COUNT(*) FROM claims"))
        print(f"Total claims in database: {result.scalar()}")

        result = conn.execute(
            text("SELECT claim_status, COUNT(*) FROM claims GROUP BY claim_status")
        )
        print("\nClaims by status:")
        for row in result:
            print(f"  {row[0]}: {row[1]}")

        result = conn.execute(
            text(
                "SELECT fraud_indicator, COUNT(*) FROM claims GROUP BY fraud_indicator"
            )
        )
        print("\nFraud indicators:")
        for row in result:
            print(f"  Fraud: {row[0]} - Count: {row[1]}")

    engine.dispose()
    print("\n✓ Data generation complete!")
    print(f"\nNow run: python src/extract_and_load.py")


if __name__ == "__main__":
    main()
