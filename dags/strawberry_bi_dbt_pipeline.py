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
    "email": ["you@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ✅ REQUIRED: you were missing this
BASE_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PATH": f"/home/airflow/.local/bin:{os.environ.get('PATH', '')}",
}

DBT_ENV = {
    **BASE_ENV,
    "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
    "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", ""),
    "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", ""),
    "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE_DBT") or os.environ.get("SNOWFLAKE_ROLE", ""),
}

def dbt(cmd: str) -> str:
    return f"cd {DBT_PROJECT_DIR} && {DBT_BIN} {cmd} --profiles-dir {DBT_PROFILES_DIR}"

with DAG(
    dag_id="strawberry_bi_dbt_pipeline",
    default_args=default_args,
    description="Strawberry BI pipeline: dbt deps -> seed -> bronze -> snapshot -> silver -> gold -> test",
    schedule_interval="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["strawberry", "dbt", "snowflake"],
) as dag:

    print_env = BashOperator(
        task_id="print_env",
        bash_command="env | sort | grep -E 'SNOWFLAKE|DBT_PROFILES_DIR|PATH' || true",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt("deps"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=5),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=dbt("seed"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=10),
    )

    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command=dbt("run --select tag:bronze"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=20),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_membership",
        bash_command=dbt("snapshot"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=30),
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt("run --select tag:silver+"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=50),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt("run --select tag:gold"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=65),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=dbt("test"),
        env=DBT_ENV,
        append_env=True,
        sla=timedelta(minutes=80),
    )

    print_env >> dbt_deps >> dbt_seed >> dbt_run_bronze >> dbt_snapshot >> dbt_run_silver >> dbt_run_gold >> dbt_test_all

