"""
End-to-end pipeline orchestrator.
Usage:
    python run_pipeline.py --step all          # run everything
    python run_pipeline.py --step scrape       # only scrape
    python run_pipeline.py --step spark        # only spark processing
    python run_pipeline.py --step dbt          # only dbt transformations
"""

import argparse
import subprocess
import sys
from pathlib import Path

from loguru import logger

# PROJECT_ROOT is two levels up from src/orchestration/run_pipeline.py
PROJECT_ROOT = Path(__file__).parent.parent.parent


def run_cmd(cmd: list[str], cwd: Path | None = None, env_extra: dict | None = None):
    import os
    env = {**os.environ, **(env_extra or {})}
    logger.info(f"▶ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd or PROJECT_ROOT), env=env)
    if result.returncode != 0:
        logger.error(f"Command failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def step_scrape():
    logger.info("=== STEP: Scraping ===")
    run_cmd(
        ["python", "src/ingestion/scrapers/shopee_scraper.py"],
        cwd=PROJECT_ROOT,
    )
    run_cmd(
        ["python", "src/ingestion/scrapers/lazada_scraper.py"],
        cwd=PROJECT_ROOT,
    )


def step_spark():
    logger.info("=== STEP: Spark Processing ===")
    # Download PostgreSQL JDBC driver automatically via Maven
    run_cmd(
        [
            "spark-submit",
            "--packages", "org.postgresql:postgresql:42.7.3",
            "src/processing/spark_pipeline.py",
        ],
        cwd=PROJECT_ROOT,
    )


def step_dbt():
    logger.info("=== STEP: dbt Transformations ===")
    dbt_dir = PROJECT_ROOT / "dbt"
    profiles_dir = str(dbt_dir)

    run_cmd(
        ["dbt", "deps", "--profiles-dir", profiles_dir],
        cwd=dbt_dir,
    )
    run_cmd(
        ["dbt", "run", "--profiles-dir", profiles_dir],
        cwd=dbt_dir,
    )
    run_cmd(
        ["dbt", "test", "--profiles-dir", profiles_dir],
        cwd=dbt_dir,
    )


def interactive_menu():
    print("\n==========================================================")
    print(" Customer Analytics Lakehouse - Pipeline Orchestrator")
    print("==========================================================")
    print("Where are you running this from? Pick a service:")
    print("\n[  MAC HOST ]")
    print("  1. Run Web Scrapers (Chrome GUI required)")
    print("\n[  UBUNTU VM ]")
    print("  2. Start Infrastructure (MinIO, Airflow, Postgres, Metabase)")
    print("  3. Stop Infrastructure")
    print("  4. Run PySpark Processing (Raw 