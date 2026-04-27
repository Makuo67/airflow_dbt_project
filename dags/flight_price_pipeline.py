from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from src.ingestion.mysql_dump import load_csv_to_mysql
from src.validation.validation import validate_raw_data
from src.ingestion.load_to_postgres_staging import load_to_postgres
from src.sql.mysql_schema import create_mysql_schema
from src.sql.postgres_staging_schema import create_postgres_schema

default_args = {
    "owner": "okeke",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="flight_price_pipelinev7",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flight", "etl", "prices"],
    description="Production ETL pipeline: CSV → MySQL → Validation → KPIs → Postgres → dbt Models → Marts",
) as dag:

    # ----------------------------
    # 1. INGESTION STEP (MySQL)
    # ----------------------------
    mysql_schema_task = PythonOperator(
        task_id="create_mysql_schema",
        python_callable=create_mysql_schema
    )

    ingest_task = PythonOperator(
        task_id="load_csv_to_mysql",
        python_callable=load_csv_to_mysql
    )

    # ----------------------------
    # 2. VALIDATION STEP
    # ----------------------------
    validate_task = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data
    )

    # ----------------------------
    # 3. POSTGRES STAGING SCHEMA
    # ----------------------------
    postgres_schema_task = PythonOperator(
        task_id="create_postgres_schema",
        python_callable=create_postgres_schema
    )

    # ----------------------------
    # 4. LOAD TO POSTGRES STAGING
    # ----------------------------
    load_postgres_task = PythonOperator(
        task_id="load_to_postgres_staging",
        python_callable=load_to_postgres
    )

    # ----------------------------
    # 5. DBT TRANSFORMATION
    # ----------------------------
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="""
        set -euxo pipefail

        echo "=== DBT DEBUG START ==="

        dbt debug \
            --project-dir /opt/airflow/dbt \
            --profiles-dir /opt/airflow/dbt \
            --log-level debug \
            --debug

        echo "=== DBT RUN START ==="

        dbt run \
            --project-dir /opt/airflow/dbt \
            --profiles-dir /opt/airflow/dbt \
            --log-level debug
    """
    )
    # # ----------------------------
    # # 6. TRANSFER MARTS TO PRODUCTION SCHEMA
    # # ----------------------------
    # transfer_marts_task = PythonOperator(
    #     task_id="transfer_marts_to_postgres",
    #     python_callable=transfer_marts_to_postgres
    # )

    # ----------------------------
    # TASK DEPENDENCIES
    # ----------------------------
    (
        mysql_schema_task
        >> ingest_task
        >> validate_task
        >> postgres_schema_task
        >> load_postgres_task
        >> dbt_run_task
    )
