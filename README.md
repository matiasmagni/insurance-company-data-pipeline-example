# Insurance Company Data Pipeline

A production-grade data lakehouse pipeline demonstrating modern ELT/ETL architecture with insurance industry data.

<div align="center">

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MinIO](https://img.shields.io/badge/MinIO-ED1C24?style=for-the-badge&logo=min-io&logoColor=white)](https://min.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![DBT](https://img.shields.io/badge/dbt-F0F0F0?style=for-the-badge&logo=dbt&logoColor=black)](https://www.getdbt.com/)
[![DLT](https://img.shields.io/badge/DLT-2E86DE?style=for-the-badge&logo=&logoColor=white)](https://dlthub.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

</div>

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Flow Diagram](#2-data-flow-diagram)
3. [Infrastructure Components](#3-infrastructure-components)
4. [Project Structure](#4-project-structure)
5. [Test Pyramid](#5-test-pyramid)
6. [Quick Start](#6-quick-start)
7. [Database Connections](#7-database-connections)
8. [Running Tests](#8-running-tests)
9. [SQL Examples](#9-sql-examples)
10. [Environment Variables](#10-environment-variables)
11. [Troubleshooting](#11-troubleshooting)
12. [License](#12-license)
13. [Author](#13-author)

---

```
Copyright (c) 2026 BugMentor (https://bugmentor.com)
Eng. Matías J. Magni | CEO @ BugMentor
```

---

## 1. Architecture Overview

This project implements a **three-tier data lakehouse architecture**:

```mermaid
graph LR
    subgraph "Layer 1: RAW SOURCE"
        PG[("PostgreSQL\nport 5432")]
    end
    
    subgraph "Layer 2: STORAGE (MinIO)"
        RAW[s3://insurance-data/raw/]
        SIL[s3://insurance-data/silver/]
    end
    
    subgraph "Layer 3: ANALYTICS (ClickHouse)"
        GOLD[("ClickHouse\nport 8123")]
    end
    
    DLT[DLT Pipeline]
    DBT[DBT Transformations]
    
    PG -->|1. Extract| DLT
    DLT -->|2. Load Parquet| RAW
    RAW -->|3. Transform| DBT
    DBT -->|4. Write Parquet| SIL
    SIL -->|5. Load Analytics| DBT
    DBT -->|6. Write Tables| GOLD
```

---

## 2. Data Flow Diagram

```mermaid
flowchart TB
    subgraph "STEP 1: Generate Source Data"
        A[go run scripts/synthetic_data_generator.go] --> B[(PostgreSQL\ncustomers & claims)]
    end
    
    subgraph "STEP 2: Extract & Load to Raw"
        C[python scripts/dlt_pipeline.py] --> D[s3://insurance-data/raw/customers/]
        C --> E[s3://insurance-data/raw/claims/]
    end
    
    subgraph "STEP 3: Transform to Silver"
        F[dbt/models/silver/] --> G[s3://insurance-data/silver/customers/]
        F --> H[s3://insurance-data/silver/claims/]
    end
    
    subgraph "STEP 4: Load to Gold"
        I[dbt/models/gold/] --> J[(ClickHouse\ncustomers)]
        I --> K[(ClickHouse\nclaims)]
        I --> L[(ClickHouse\nclaims_by_status)]
        I --> M[(ClickHouse\nclaims_by_agent)]
    end
    
    B --> C
    D --> F
    E --> F
    G --> I
    H --> I
```

---

## 3. Infrastructure Components

```mermaid
graph TB
    subgraph "Docker Compose Services"
        PG[(PostgreSQL\n:5432)]:::db
        MINIO[(MinIO\n:9900/:9901)]:::storage
        CH[(ClickHouse\n:8123/:9000)]:::analytics
        PGADMIN[(pgAdmin\n:8080)]:::admin
    end
    
    classDef db fill:#f9f,stroke:#333
    classDef storage fill:#ff9,stroke:#333
    classDef analytics fill:#9f9,stroke:#333
    classDef admin fill:#9ff,stroke:#333
```

| Service | Port | Purpose | Schema |
|---------|------|---------|--------|
| **PostgreSQL** | 5432 | Raw source data | `public.customers`, `public.claims` |
| **MinIO** | 9900 (API), 9901 (Console) | Raw + Silver storage | `s3://insurance-data/{raw,silver}/` |
| **ClickHouse** | 8123 (HTTP), 9000 (Native) | Gold analytics | Table names: `customers`, `claims`, `claims_by_status` |
| **pgAdmin** | 8080 | PostgreSQL admin UI | - |

---

## 4. Project Structure

```
insurance-company-data-pipeline-example/
├── docker-compose.yml           # Infrastructure orchestration
├── pipeline.py                  # Main pipeline runner (DLT + DBT)
├── README.md                    # This file
│
├── scripts/                    # Python/Go scripts
│   ├── synthetic_data_generator.go  # Go data generator (1000 customers, 5000 claims)
│   ├── dlt_pipeline.py             # DLT pipeline: PostgreSQL → MinIO raw
│   ├── download_kaggle_data.py     # Download Kaggle insurance dataset
│   └── reset_database.go           # Reset PostgreSQL and regenerate data
│
├── dbt/                        # DBT project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml           # MinIO source definitions
│       ├── silver/
│       │   ├── silver_customers.sql
│       │   └── silver_claims.sql
│       └── gold/
│           ├── gold_customers.sql
│           ├── gold_claims.sql
│           ├── gold_claims_by_status.sql
│           ├── gold_claims_by_agent.sql
│           └── gold_claims_by_business_line.sql
│
├── scripts_unix/                # Unix test runners
│   ├── run_l0_tests.sh          # Unit tests isolated
│   ├── run_l1_tests.sh          # Unit tests integrated
│   ├── run_l2_tests.sh          # Integration tests
│   ├── run_l3_tests.sh          # E2E tests
│   └── run_all_tests.sh         # Run all tests
│
├── scripts_windows/             # Windows test runners
│   ├── run_l0_tests.bat        # Unit tests isolated
│   ├── run_l1_tests.bat        # Unit tests integrated
│   ├── run_l2_tests.bat        # Integration tests
│   ├── run_l3_tests.bat        # E2E tests
│   └── run_all_tests.bat       # Run all tests
│
└── tests/                      # Test suite (77 tests)
    ├── test_L0_unit_isolated.py    # L0: Isolated unit tests (30)
    ├── test_L1_unit_integrated.py  # L1: Integrated unit tests (18)
    ├── test_L2_integration.py     # L2: Integration tests (19)
    └── test_L3_e2e.py           # L3: End-to-end tests (10)
```

---

## 5. Test Pyramid

This project follows the **test pyramid** methodology with four levels of testing:

```mermaid
block-beta
columns 13

space:4 L3["🔴 L3: End-to-End<br/>Full Pipeline"]:5 space:4
space:3 L2["🟠 L2: Integration Services<br/>Real Services / Local Docker"]:7 space:3
space:1 L1["🟡 L1: Unit with Mocks<br/>Mocked Dependencies / Partial Mocks"]:11 space:1
L0["🟢 L0: Unit Isolation (Most Tests)<br/>Pure Functions / No I/O / No Dependencies"]:13

style L3 fill:#ff5722,color:#fff,stroke:#333,stroke-width:2px
style L2 fill:#ff9800,color:#fff,stroke:#333,stroke-width:2px
style L1 fill:#ffc107,color:#000,stroke:#333,stroke-width:2px
style L0 fill:#ffeb3b,color:#000,stroke:#333,stroke-width:2px
```

### 5.1. Test Levels Description

| Level | Name | Description |
|-------|------|-------------|
| **L0** | Unit Isolation | Pure functions, no I/O, no dependencies - most tests |
| **L1** | Unit with Mocks | Mocked dependencies, partial mocks - more tests |
| **L2** | Integration Services | Real services, local Docker - some tests |
| **L3** | End-to-End | Full pipeline - fewest tests |

---

## 6. Quick Start

### 6.1. Start Infrastructure

```bash
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 6.2. Generate Source Data

```bash
# Using Go synthetic data generator (1000 customers, 5000 claims)
go run scripts/synthetic_data_generator.go
```

### 6.3. Reset & Regenerate Database

```bash
# Reset database (truncate all tables)
go run scripts/reset_database.go

# Reset and regenerate data
go run scripts/reset_database.go --regenerate
```

### 6.4. Run Pipeline

```bash
# Run full pipeline: PostgreSQL → MinIO (raw) → MinIO (silver) → ClickHouse (gold)
python pipeline.py

# Or run steps individually:
python scripts/dlt_pipeline.py       # PostgreSQL → MinIO raw
cd dbt && dbt run                  # MinIO raw → MinIO silver → ClickHouse gold
```

---

## 7. Database Connections

### 7.1. PostgreSQL (Raw Source)

| Parameter | Value |
|-----------|-------|
| Host | localhost |
| Port | **5432** |
| Database | insurance_db |
| Username | insurance_user |
| Password | insurance_pass |

**Tables:**
- `public.customers` - Customer profiles (1,000 rows)
- `public.claims` - Insurance claims (5,000 rows)

### 7.2. MinIO (Raw + Silver Layers)

| Parameter | Value |
|-----------|-------|
| Host | localhost |
| Port | **9900** (API) |
| Console | http://localhost:9901 |
| Bucket | insurance-data |
| Access Key | minioadmin |
| Secret Key | minioadmin |

**Path Structure:**
```
s3://insurance-data/
├── raw/
│   ├── customers/
│   │   └── customers_001.parquet
│   └── claims/
│       └── claims_001.parquet
└── silver/
    ├── customers/
    │   └── customers_enriched.parquet
    └── claims/
        └── claims_with_agents.parquet
```

### 7.3. ClickHouse (Gold Layer)

| Parameter | Value |
|-----------|-------|
| Host | localhost |
| Port | **8123** (HTTP) or 9000 (Native) |
| Database | insurance_db |
| Username | default |
| Password | clickhouse_pass |

**Gold Tables (NO schema prefix - just table names):**
- `customers` - Enriched customer data with risk buckets
- `claims` - Claims with agent group assignments
- `claims_by_status` - Aggregated by claim status
- `claims_by_agent` - Aggregated by agent group
- `claims_by_business_line` - Aggregated by vehicle type

---

## 8. Running Tests

### 8.1. Unix/Linux/macOS

```bash
# Run all tests
./scripts_unix/run_all_tests.sh

# Run specific test levels
./scripts_unix/run_l0_tests.sh   # Unit tests - isolated
./scripts_unix/run_l1_tests.sh   # Unit tests - integrated
./scripts_unix/run_l2_tests.sh  # Integration tests
./scripts_unix/run_l3_tests.sh  # End-to-end tests
```

### 8.2. Windows

```cmd
REM Run all tests
scripts_windows\run_all_tests.bat

REM Run specific test levels
scripts_windows\run_l0_tests.bat   -- Unit tests - isolated
scripts_windows\run_l1_tests.bat   -- Unit tests - integrated
scripts_windows\run_l2_tests.bat  -- Integration tests
scripts_windows\run_l3_tests.bat   -- End-to-end tests
```

### 8.3. Direct pytest

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_L0_unit_isolated.py -v
pytest tests/test_L1_unit_integrated.py -v
pytest tests/test_L2_integration.py -v
pytest tests/test_L3_e2e.py -v
```

---

## 9. SQL Examples

### 9.1. ClickHouse Gold Layer

All tables in ClickHouse are accessed **without** schema prefix:

```sql
-- View all customers
SELECT * FROM customers LIMIT 10;

-- View all claims
SELECT * FROM claims LIMIT 10;

-- Claims by status
SELECT * FROM claims_by_status;

-- Claims by agent
SELECT * FROM claims_by_agent;

-- Claims by business line (vehicle type)
SELECT * FROM claims_by_business_line;

-- Analytics: Open vs Closed claims
SELECT 
    claim_status,
    COUNT(*) AS claim_count,
    SUM(claim_paid_amount) AS total_paid
FROM claims
GROUP BY claim_status;

-- Customer risk analysis
SELECT 
    risk_bucket,
    COUNT(*) AS customer_count,
    AVG(credit_score) AS avg_credit_score
FROM customers
GROUP BY risk_bucket
ORDER BY customer_count DESC;
```

---

## 10. Environment Variables

Create a `.env` file in the project root:

```bash
# PostgreSQL
POSTGRES_USER=insurance_user
POSTGRES_PASSWORD=insurance_pass
POSTGRES_DB=insurance_db

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=insurance-data

# ClickHouse
CLICKHOUSE_DB=insurance_db
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse_pass
```

---

## 11. Troubleshooting

### 11.1. Check MinIO Console

1. Open http://localhost:9901 in browser
2. Login with: minioadmin / minioadmin
3. Verify bucket `insurance-data` exists with `raw/` and `silver/` folders

### 11.2. Check ClickHouse

```bash
# Using clickhouse-client
docker exec -it insurance_clickhouse clickhouse-client

# Check tables
SHOW TABLES;

-- Should show: customers, claims, claims_by_status, claims_by_agent, claims_by_business_line
```

### 11.3. Check PostgreSQL

```bash
# Connect to PostgreSQL
docker exec -it insurance_postgres psql -U insurance_user -d insurance_db

-- Check tables
/dt

-- Should show: customers, claims
```

---

## 12. License

Copyright (c) 2026 BugMentor (https://bugmentor.com)

---

## 13. Author

**Eng. Matías J. Magni**  
CEO @ BugMentor  
https://bugmentor.com