import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from libs.external_api_client import fetch_loyalty_events
from libs.snowflake_loader import ensure_external_loyalty_table_exists, load_events_jsonl_via_copy_into

DAG_ID = "external_api_to_snowflake_backfill"
SOURCE = "external_loyalty_api_backfill"


def _parse_iso_utc(s: str) -> datetime:
    """
    Accepts '2026-01-01T00:00:00Z' or '2026-01-01T00:00:00+00:00'
    """
    s = (s or "").strip()
    if not s:
        raise AirflowFailException("Empty timestamp string")
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _require_conf_window(**context) -> Tuple[datetime, datetime]:
    """
    Require start/end from dag_run.conf.
    This is the most reliable way (works from UI trigger config box).
    """
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf) if dag_run and dag_run.conf else {}

    start_ts = conf.get("start_ts")
    end_ts = conf.get("end_ts")

    if not start_ts or not end_ts:
        raise AirflowFailException(
            "Missing conf keys start_ts and end_ts.\n"
            'Trigger with:\n'
            '{"start_ts":"2026-01-01T00:00:00Z","end_ts":"2026-01-02T00:00:00Z"}'
        )

    since = _parse_iso_utc(start_ts)
    until = _parse_iso_utc(end_ts)

    if until <= since:
        raise AirflowFailException(f"end_ts must be > start_ts (got {since} .. {until})")

    return since, until


def _write_jsonl(objs: List[Dict[str, Any]], out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in objs:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def backfill_fetch_load(**context) -> None:
    """
    Backfill-only DAG:
      - window comes from dag_run.conf (start_ts/end_ts)
      - loads into RAW
      - does NOT update RAW.LOAD_STATE (no watermark movement)
    """
    database = os.environ.get("SNOWFLAKE_DATABASE", "STRAWBERRY")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
    run_id = context.get("run_id") or str(uuid.uuid4())

    window_since, window_until = _require_conf_window(**context)

    ensure_external_loyalty_table_exists(database=database, schema=schema)

    ingestion_id = str(uuid.uuid4())
    rows_loaded = 0

    # NOTE: your current demo client is since-only; OK for now.
    # If/when your real API supports an upper bound, update the client and call:
    # raw_events = fetch_loyalty_events(since=window_since, until=window_until)
    raw_events = fetch_loyalty_events(since=window_since)

    jsonl_path = f"/tmp/strawberry/external_loyalty/backfill_{ingestion_id}.jsonl"
    _write_jsonl(raw_events, jsonl_path)

    _, rows_loaded = load_events_jsonl_via_copy_into(
        database=database,
        schema=schema,
        jsonl_path_in_container=jsonl_path,
        source=SOURCE,
        ingestion_id=ingestion_id,
    )

    print(
        f"BACKFILL SUCCESS: Loaded {rows_loaded} rows into {schema}.EXTERNAL_LOYALTY_EVENTS_RAW "
        f"(ingestion_id={ingestion_id}, run_id={run_id}, window={window_since}..{window_until})"
    )


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(seconds=10)},
    tags=["backfill", "snowflake", "external_api"],
) as dag:
    PythonOperator(
        task_id="backfill_fetch_load_loyalty_events_raw",
        python_callable=backfill_fetch_load,
        provide_context=True,
    )

