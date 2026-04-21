from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'customer_lakehouse_pipeline',
    default_args=default_args,
    description='Automated orchestration of PySpark processing and dbt mart generation',
    schedule_interval='0 2 * * *',  # Run daily at 2:00 AM
    catchup=False,
    tags=['lakehouse', 'spark', 'dbt'],
) as dag:

    # 1. S3 Sensor - Wait for new data in MinIO raw-data bucket
    # Note: Requires AWS connection named 'aws_default' configured in Airflow
    # pointing to MinIO endpoint.
    wait_for_raw_data = S3KeySensor(
        task_id='wait_for_raw_data',
        bucket_key='shopee/*.parquet',
        wildcard_match=True,
        bucket_name='raw-data',
        aws_conn_id='aws_default',
        timeout=60 * 60 * 12,  # wait up to 12 hours
        poke_interval=60 * 5,  # check every 5 minutes
        mode='reschedule'
    )

    # 2. Trigger the PySpark Pipeline
    # Requires PySpark built into the Airflow container or SparkSubmitOperator
    run_spark_processing = BashOperator(
        task_id='run_spark_processing',
        bash_command='python /opt/app/src/orchestration/run_pipeline.py --step spark',
    )

    # 3. Trigger the dbt Transformations
    # Requires dbt-postgres installed in the Airflow container
    run_dbt_transformations = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='python /opt/app/src/orchestration/run_pipeline.py --step dbt',
    )

    wait_for_raw_data >> run_spark_processing >> run_dbt_transformations
