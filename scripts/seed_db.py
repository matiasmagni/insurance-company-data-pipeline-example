#!/usr/bin/env python3
import os
import psycopg2
import random
from datetime import datetime, timedelta


def seed_db():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5435")),
        user=os.getenv("POSTGRES_USER", "insurance_user"),
        password=os.getenv("POSTGRES_PASSWORD", "insurance_pass"),
        database=os.getenv("POSTGRES_DB", "insurance_db"),
    )
    cur = conn.cursor()

    print("Creating tables...")
    cur.execute("""
        DROP TABLE IF EXISTS claims CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;

        CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255) UNIQUE,
            phone_number VARCHAR(20),
            date_of_birth DATE,
            address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(2),
            zip_code VARCHAR(10),
            country VARCHAR(100) DEFAULT 'USA',
            credit_score INTEGER,
            annual_income DECIMAL(12,2),
            occupation VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE claims (
            claim_id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(customer_id),
            claim_date DATE,
            claim_type VARCHAR(50),
            claim_status VARCHAR(20),
            claim_amount DECIMAL(12,2),
            claim_paid_amount DECIMAL(12,2),
            vehicle_type VARCHAR(20),
            agent_id INTEGER,
            agent_name VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    first_names = [
        "James",
        "Mary",
        "John",
        "Patricia",
        "Robert",
        "Jennifer",
        "Michael",
        "Linda",
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
    ]
    occupations = ["Engineer", "Doctor", "Lawyer", "Teacher", "Accountant"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston"]
    claim_types = ["Auto", "Home", "Life", "Health"]
    claim_statuses = ["Open", "Closed", "Pending", "Denied"]
    vehicle_types = ["Sedan", "SUV", "Truck", "Motorcycle"]

    print("Generating 100 customers...")
    for i in range(100):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        cur.execute(
            """
            INSERT INTO customers (first_name, last_name, email, credit_score, annual_income, occupation, city, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
        """,
            (
                fn,
                ln,
                email,
                random.randint(500, 850),
                random.uniform(30000, 200000),
                random.choice(occupations),
                random.choice(cities),
                "NY",
            ),
        )
        cust_id = cur.fetchone()[0]

        for _ in range(random.randint(1, 5)):
            amount = random.uniform(1000, 50000)
            cur.execute(
                """
                INSERT INTO claims (customer_id, claim_date, claim_type, claim_status, claim_amount, claim_paid_amount, vehicle_type, agent_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    cust_id,
                    datetime.now() - timedelta(days=random.randint(0, 365)),
                    random.choice(claim_types),
                    random.choice(claim_statuses),
                    amount,
                    amount if random.random() > 0.5 else 0,
                    random.choice(vehicle_types),
                    "Agent Smith",
                ),
            )

    conn.commit()
    cur.close()
    conn.close()
    print("Database seeded successfully!")


if __name__ == "__main__":
    seed_db()
