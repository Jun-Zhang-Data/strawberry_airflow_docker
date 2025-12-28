from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from libs.external_api_client import fetch_loyalty_events
from libs.transformations import normalize_loyalty_events
from libs.snowflake_loader import ensure_external_loyalty_table_exists, insert_loyalty_events


DAG_ID = "external_api_to_snowflake"


def fetch_transform_load(**context):
    # Fetch events updated in the last 24 hours
    since = datetime.utcnow() - timedelta(days=1)

    # 1) Fetch from external API
    raw_events = fetch_loyalty_events(since=since)

    # 2) Transform / normalize
    normalized = normalize_loyalty_events(raw_events)

    # 3) Ensure target table exists + load into Snowflake
    ensure_external_loyalty_table_exists()
    inserted = insert_loyalty_events(normalized)

    print(f"Inserted {inserted} loyalty events into RAW.EXTERNAL_LOYALTY_EVENTS")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["external_api", "snowflake", "dbt"],
) as dag:

    fetch_and_load = PythonOperator(
        task_id="fetch_transform_load_loyalty_events",
        python_callable=fetch_transform_load,
        provide_context=True,
    )
