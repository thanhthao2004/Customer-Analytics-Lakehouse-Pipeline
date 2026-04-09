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

PROJECT_ROOT = Path(__file__).parent


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
        ["python", "scrapers/shopee_scraper.py"],
        cwd=PROJECT_ROOT,
    )
    run_cmd(
        ["python", "scrapers/lazada_scraper.py"],
        cwd=PROJECT_ROOT,
    )


def step_spark():
    logger.info("=== STEP: Spark Processing ===")
    # Download PostgreSQL JDBC driver automatically via Maven
    run_cmd(
        [
            "spark-submit",
            "--packages", "org.postgresql:postgresql:42.7.3",
            "spark/pipeline.py",
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
    print("  4. Run PySpark Processing (Raw Data -> Clean PostgreSQL)")
    print("  5. Run dbt Transformations (Warehouse -> Data Marts)")
    print("\n  0. Exit")
    print("==========================================================")
    
    try:
        choice = input("Enter your choice [0-5]: ").strip()
    except KeyboardInterrupt:
        print("\nExiting.")
        sys.exit(0)
        
    if choice == '1':
        step_scrape()
    elif choice == '2':
        logger.info("=== STEP: Start Docker Infra ===")
        run_cmd(["docker", "compose", "up", "-d"], cwd=PROJECT_ROOT)
    elif choice == '3':
        logger.info("=== STEP: Stop Docker Infra ===")
        run_cmd(["docker", "compose", "down"], cwd=PROJECT_ROOT)
    elif choice == '4':
        step_spark()
    elif choice == '5':
        step_dbt()
    elif choice == '0':
        print("Exiting.")
        sys.exit(0)
    else:
        print("Invalid choice, please select 0-5.")

def main():
    parser = argparse.ArgumentParser(description="Customer Analytics Pipeline Orchestrator")
    parser.add_argument(
        "--step",
        choices=["all", "scrape", "spark", "dbt", "interactive"],
        default="interactive",
        help="Which step to run (default: interactive CLI GUI)",
    )
    args = parser.parse_args()

    logger.add(
        PROJECT_ROOT / "logs" / "orchestrator_{time}.log",
        rotation="50 MB",
        level="INFO",
    )

    if args.step == "interactive":
        while True:
            interactive_menu()
    else:
        if args.step in ("all", "scrape"):
            step_scrape()
        if args.step in ("all", "spark"):
            step_spark()
        if args.step in ("all", "dbt"):
            step_dbt()

        logger.success("🎉 Pipeline complete!")


if __name__ == "__main__":
    main()
