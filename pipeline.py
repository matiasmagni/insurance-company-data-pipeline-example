#!/usr/bin/env python3
"""
Insurance Company Data Pipeline
=============================
Pipeline: PostgreSQL + Kaggle → MinIO(raw) → MinIO(silver) → ClickHouse(gold)

Architecture:
    PostgreSQL ─┐
                ├──→ MinIO raw/ ──→ MinIO silver/ ──→ ClickHouse gold
    Kaggle CSV ─┘     (DLT)          (Python)       (DBT)

Usage:
    python pipeline.py
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log")
    ]
)
logger = logging.getLogger(__name__)


def run_script(script_name: str, cwd: Path = None) -> bool:
    """Run a python script and return success/failure."""
    script_path = Path(__file__).parent / "scripts" / script_name
    logger.info("-" * 60)
    logger.info(f"Running script: {script_name}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(cwd) if cwd else str(Path(__file__).parent),
            check=True,
            capture_output=False
        )
        logger.info(f"Script {script_name} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Script {script_name} failed with exit code {e.returncode}")
        return False
    except Exception as e:
        logger.error(f"Error running {script_name}: {e}")
        return False


def main():
    """Run the full pipeline."""
    logger.info("=" * 60)
    logger.info("Insurance Company Data Pipeline")
    logger.info("=" * 60)
    
    steps = [
        ("Step 0: Download Kaggle data", "download_kaggle_data.py"),
        ("Step 1a: Extract PG to MinIO (DLT)", "dlt_pipeline.py"),
        ("Step 1b: Extract Kaggle to MinIO", "load_kaggle_to_minio.py"),
        ("Step 2: Transform Raw to Silver (Python)", "silver_transform.py"),
        ("Step 2.5: Load Silver to ClickHouse", "load_silver_to_clickhouse.py"),
    ]

    for description, script in steps:
        logger.info(f"\n>>> {description}")
        if not run_script(script):
            logger.error(f"Pipeline stopped at {description}")
            sys.exit(1)

    # Step 3: DBT Gold
    logger.info("\n>>> Step 3: DBT Gold Aggregations")
    dbt_dir = Path(__file__).parent / "dbt"
    try:
        subprocess.run(
            ["dbt", "run"],
            cwd=str(dbt_dir),
            check=True
        )
        logger.info("DBT completed successfully")
    except Exception as e:
        logger.warning(f"DBT failed ({e}), falling back to direct ClickHouse queries...")
        try:
            import clickhouse_connect
            ch = clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                database=os.getenv("CLICKHOUSE_DB", "insurance_db"),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_pass"),
            )
            
            # gold_customers
            ch.command("CREATE OR REPLACE VIEW gold_customers AS SELECT * FROM silver_customers")
            
            # gold_claims
            ch.command("CREATE OR REPLACE VIEW gold_claims AS SELECT * FROM silver_claims")
            
            # gold_claims_by_status
            ch.command("""
                CREATE OR REPLACE VIEW gold_claims_by_status AS 
                SELECT 
                    claim_status_category,
                    COUNT(*) AS total_claims,
                    SUM(claim_amount) AS total_amount,
                    AVG(claim_amount) AS avg_claim_amount
                FROM silver_claims
                GROUP BY claim_status_category
            """)
            
            # gold_claims_by_agent
            ch.command("""
                CREATE OR REPLACE VIEW gold_claims_by_agent AS 
                SELECT 
                    agent_name as agent_id,
                    COUNT(*) AS total_claims,
                    SUM(claim_amount) AS total_amount,
                    AVG(claim_amount) AS avg_claim_amount
                FROM silver_claims
                GROUP BY agent_name
            """)
            
            # gold_claims_by_business_line
            ch.command("""
                CREATE OR REPLACE VIEW gold_claims_by_business_line AS 
                SELECT 
                    claim_type AS business_line,
                    COUNT(*) AS total_claims,
                    SUM(claim_amount) AS total_amount
                FROM silver_claims
                GROUP BY claim_type
            """)
            
            logger.info("Fallback gold aggregations completed successfully via direct SQL")
        except Exception as fallback_e:
            logger.error(f"Fallback also failed: {fallback_e}")
            sys.exit(1)

    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
