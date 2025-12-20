from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# Paths inside the container
DBT_PROJECT_DIR = "/opt/airflow/strawberry_bi"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"


default_args = {
    "owner": "junz",
    "depends_on_past": False,
    "email": ["you@example.com"],  # optional; can be []
    "email_on_failure": False,     # turn off for now to avoid SMTP noise
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Base env for all dbt commands – add DBT_PROFILES_DIR and ensure dbt is on PATH
BASE_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PATH": f"/home/airflow/.local/bin:{os.environ.get('PATH', '')}",
}


with DAG(
    dag_id="strawberry_bi_dbt_pipeline",
    default_args=default_args,
    description="Strawberry BI pipeline: dbt deps -> seed -> bronze -> snapshot -> silver -> gold -> test",
    schedule_interval="0 5 * * *",  # every day at 05:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["strawberry", "dbt", "snowflake"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps",
        env=BASE_ENV,
        sla=timedelta(minutes=5),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} seed",
        env=BASE_ENV,
        sla=timedelta(minutes=10),
    )

    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select tag:bronze",
        env=BASE_ENV,
        sla=timedelta(minutes=20),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_membership",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} snapshot",
        env=BASE_ENV,
        sla=timedelta(minutes=30),
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select tag:silver+",
        env=BASE_ENV,
        sla=timedelta(minutes=50),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select tag:gold",
        env=BASE_ENV,
        sla=timedelta(minutes=65),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test",
        env=BASE_ENV,
        sla=timedelta(minutes=80),
    )

    dbt_deps >> dbt_seed >> dbt_run_bronze >> dbt_snapshot >> dbt_run_silver >> dbt_run_gold >> dbt_test_all
