"""
Airflow DAG for Customer Experience Analytics Pipeline

This DAG orchestrates the end-to-end data flow:
1. Scrape data from Shopee & Lazada
2. Process with PySpark (cleaning, enrichment, Gemini sentiment, and load to Postgre warehouse)
3. Transform with dbt
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# Get the path to the project root assuming airflow runs in the same environment
# In production, adapt ROOT_DIR appropriately.
ROOT_DIR = os.getenv("PROJECT_ROOT", "/opt/airflow/project")

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'customer_analytics_pipeline',
    default_args=default_args,
    description='End-to-end ETL for Customer Experience Analytics',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'scraping', 'dbt', 'spark'],
) as dag:

    # 1. Scrape data
    # (Running the standalone scraper script)
    task_scrape_reviews = BashOperator(
        task_id='scrape_reviews',
        bash_command=f'python3 {ROOT_DIR}/scraper/scrape_reviews.py',
    )

    # 2. Process data with Spark & load to PostgreSQL
    # This script reads raw Parquet, attaches Gemini sentiment, and loads to DB.
    task_spark_processing = BashOperator(
        task_id='process_with_spark',
        bash_command=f'python3 {ROOT_DIR}/spark/pipeline.py',
    )

    # 3. Transform data using dbt (Staging -> Warehouse -> Marts)
    task_dbt_run = BashOperator(
        task_id='dbt_transform',
        bash_command=f'cd {ROOT_DIR}/dbt && dbt run --profiles-dir .',
    )

    # Note: Metabase automatically picks up new data from PostgreSQL tables, 
    # so no explicit refresh task is needed here.

    # ---------------------------------------------------------
    # Define Dependencies
    # ---------------------------------------------------------
    task_scrape_reviews >> task_spark_processing >> task_dbt_run
