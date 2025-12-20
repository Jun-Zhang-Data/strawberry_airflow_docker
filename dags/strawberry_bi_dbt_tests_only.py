from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/strawberry_bi"
DBT_PROFILES_DIR = "/home/airflow/.dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"


default_args = {
    "owner": "junz",
    "depends_on_past": False,
    "email": ["you@example.com"],  # or []
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

BASE_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PATH": f"/home/airflow/.local/bin:{os.environ.get('PATH', '')}",
}


with DAG(
    dag_id="strawberry_bi_dbt_tests_only",
    default_args=default_args,
    description="Strawberry BI health-check: run dbt test only",
    schedule_interval="0 * * * *",  # every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["strawberry", "dbt", "healthcheck"],
) as dag:

    dbt_test_only = BashOperator(
        task_id="dbt_test_only",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test",
        env=BASE_ENV,
        sla=timedelta(minutes=20),
    )
