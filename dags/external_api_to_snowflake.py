import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator

from libs.external_api_client import fetch_loyalty_events
from libs.snowflake_loader import (
    ensure_external_loyalty_table_exists,
    ensure_load_state_table_exists,
    get_last_success_ts,
    load_events_jsonl_via_copy_into,
    upsert_load_state,
)

DAG_ID = "external_api_to_snowflake"
SOURCE = "external_loyalty_api"


def _parse_iso_utc(s: str) -> datetime:
    """
    Accepts '2026-01-01T00:00:00Z' or '2026-01-01T00:00:00+00:00'
    """
    s = s.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_window(**context) -> Tuple[datetime, datetime]:
    """
    Window priority:
      1) Env backfill: BACKFILL_START_TS + BACKFILL_END_TS
      2) Backfill conf: start_ts + end_ts
      3) Watermark from LOAD_STATE (last_success_ts -> now)
      4) Default: last 24 hours
    """
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)

    # 1) ENV backfill (your easy method)
    env_start = os.environ.get("BACKFILL_START_TS", "").strip()
    env_end = os.environ.get("BACKFILL_END_TS", "").strip()
    if env_start and env_end:
        since = _parse_iso_utc(env_start)
        until = _parse_iso_utc(env_end)
        return since, until

    # 2) CONF backfill
    conf = {}
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        conf = dict(dag_run.conf)

    start_ts = conf.get("start_ts")
    end_ts = conf.get("end_ts")
    if start_ts and end_ts:
        since = _parse_iso_utc(start_ts)
        until = _parse_iso_utc(end_ts)
        return since, until

    # 3) Watermark
    database = os.environ.get("SNOWFLAKE_DATABASE", "STRAWBERRY")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
    last = get_last_success_ts(database=database, schema=schema, pipeline=DAG_ID, source=SOURCE)

    if last is None:
        return now_utc - timedelta(days=1), now_utc

    return last, now_utc



def _write_jsonl(objs: List[Dict[str, Any]], out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in objs:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def fetch_transform_load(**context):
    """
    NOTE:
    - We load RAW events as VARIANT payload.
    - Normalization belongs downstream (dbt Bronze/Silver).
    """
    database = os.environ.get("SNOWFLAKE_DATABASE", "STRAWBERRY")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
    run_id = context.get("run_id") or str(uuid.uuid4())

    # Ensure Snowflake objects exist
    ensure_external_loyalty_table_exists()
    ensure_load_state_table_exists()

    window_since, window_until = _get_window(**context)

    ingestion_id = str(uuid.uuid4())
    rows_loaded = 0

    try:
        # 1) Fetch raw events (API)
        # If your client supports until, pass it; otherwise since-only is fine.
        raw_events = fetch_loyalty_events(since=window_since)

        # 2) Write JSONL in the Airflow container filesystem
        jsonl_path = f"/tmp/strawberry/external_loyalty/{ingestion_id}.jsonl"
        _write_jsonl(raw_events, jsonl_path)

        # 3) PUT + COPY INTO -> <schema>.EXTERNAL_LOYALTY_EVENTS_RAW (PAYLOAD VARIANT)
        _, rows_loaded = load_events_jsonl_via_copy_into(
            database=database,
            schema=schema,
            jsonl_path_in_container=jsonl_path,
            source=SOURCE,
            ingestion_id=ingestion_id,
        )

        # 4) Update LOAD_STATE watermark ONLY on success
        upsert_load_state(
            database=database,
            schema=schema,
            pipeline=DAG_ID,
            source=SOURCE,
            last_success_ts_utc=window_until,
            last_run_id=run_id,
            last_ingestion_id=ingestion_id,
            last_rows_loaded=rows_loaded,
            last_status="SUCCESS",
            last_message="Loaded via PUT/COPY INTO",
        )

        print(
            f"SUCCESS: Loaded {rows_loaded} rows into {schema}.EXTERNAL_LOYALTY_EVENTS_RAW "
            f"(ingestion_id={ingestion_id}, window={window_since}..{window_until})"
        )

    except Exception as e:
        # Record failure, but DO NOT move watermark
        upsert_load_state(
            database=database,
            schema=schema,
            pipeline=DAG_ID,
            source=SOURCE,
            last_success_ts_utc=None,  # keep existing watermark
            last_run_id=run_id,
            last_ingestion_id=ingestion_id,
            last_rows_loaded=rows_loaded,
            last_status="FAILED",
            last_message=str(e),
        )
        raise


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["external_api", "snowflake", "dbt"],
) as dag:
    fetch_and_load = PythonOperator(
        task_id="fetch_load_loyalty_events_raw",
        python_callable=fetch_transform_load,
        provide_context=True,
    )

