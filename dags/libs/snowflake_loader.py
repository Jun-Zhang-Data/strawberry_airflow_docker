# /opt/airflow/dags/libs/snowflake_loader.py
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple

import snowflake.connector


# -----------------------------
# Helpers
# -----------------------------
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _validate_identifier(name: str, value: str) -> str:
    """
    Prevent accidental SQL injection via env vars like database/schema.
    Only allow simple Snowflake identifiers: letters, digits, underscore.
    """
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {name} identifier: {value!r}")
    return value


def _database() -> str:
    return _validate_identifier("SNOWFLAKE_DATABASE", _require_env("SNOWFLAKE_DATABASE"))


def _schema() -> str:
    return _validate_identifier("SNOWFLAKE_SCHEMA", os.environ.get("SNOWFLAKE_SCHEMA", "RAW"))


def _get_snowflake_connection():
    """
    Create a Snowflake connection using env vars.
    Note: database/schema set here are "defaults" only; we still USE them explicitly in queries.
    """
    return snowflake.connector.connect(
        account=_require_env("SNOWFLAKE_ACCOUNT"),
        user=_require_env("SNOWFLAKE_USER"),
        password=_require_env("SNOWFLAKE_PASSWORD"),
        role=os.environ.get("SNOWFLAKE_ROLE_AIRFLOW") or os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=_require_env("SNOWFLAKE_WAREHOUSE"),
        database=_database(),
        schema=_schema(),
    )


# ============================================================
# 1) RAW ingestion: VARIANT payload using PUT + COPY INTO
# ============================================================
def ensure_external_loyalty_table_exists(database: Optional[str] = None, schema: Optional[str] = None) -> None:
    """
    Create <schema>.EXTERNAL_LOYALTY_EVENTS_RAW if missing.
    Assumes database/schema already exist (no CREATE DATABASE/SCHEMA here).
    """
    database = _validate_identifier("database", database or _database())
    schema = _validate_identifier("schema", schema or _schema())

    ddl_table = f"""
    CREATE TABLE IF NOT EXISTS {schema}.EXTERNAL_LOYALTY_EVENTS_RAW (
        INGESTION_ID   STRING,
        SOURCE         STRING,
        FILE_NAME      STRING,
        LOAD_TS_UTC    TIMESTAMP_NTZ,
        PAYLOAD        VARIANT
    )
    """

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE DATABASE {database}")
            cur.execute(f"USE SCHEMA {schema}")
            cur.execute(ddl_table)
        conn.commit()
    finally:
        conn.close()


def load_events_jsonl_via_copy_into(
    database: str,
    schema: str,
    jsonl_path_in_container: str,
    source: str,
    ingestion_id: Optional[str] = None,
) -> Tuple[str, int]:
    """
    Upload a JSONL file from the Airflow container and load it into the RAW VARIANT table.

    Key rules (Snowflake connector gotchas):
      - PUT syntax is: PUT 'file:///path' @%TABLE ...
        (NO "FROM")
      - If you call cursor.execute(sql, params), any literal '%' inside sql must be written as '%%'
        otherwise Python string formatting treats it as a placeholder and you get:
        "TypeError: not enough arguments for format string"
      - Therefore:
          * PUT (no params) uses @%EXTERNAL_LOYALTY_EVENTS_RAW
          * COPY (has params) uses @%%EXTERNAL_LOYALTY_EVENTS_RAW
    """
    database = _validate_identifier("database", database)
    schema = _validate_identifier("schema", schema)

    ingestion_id = ingestion_id or str(uuid.uuid4())

    ensure_external_loyalty_table_exists(database=database, schema=schema)

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE DATABASE {database}")
            cur.execute(f"USE SCHEMA {schema}")

            # 1) PUT local file -> internal table stage (table stage for EXTERNAL_LOYALTY_EVENTS_RAW)
            # PUT must not be parameterized (no params passed), so use single %.
            put_stage = "@%EXTERNAL_LOYALTY_EVENTS_RAW"
            put_sql = (
                f"PUT 'file://{jsonl_path_in_container}' "
                f"{put_stage} "
                f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )
            cur.execute(put_sql)

            # 2) COPY INTO table
            # COPY is parameterized (we pass ingestion_id, source) so any literal '%' must be written as '%%'.
            copy_sql = f"""
            COPY INTO {schema}.EXTERNAL_LOYALTY_EVENTS_RAW
              (INGESTION_ID, SOURCE, FILE_NAME, LOAD_TS_UTC, PAYLOAD)
            FROM (
              SELECT
                %s AS INGESTION_ID,
                %s AS SOURCE,
                METADATA$FILENAME AS FILE_NAME,
                CURRENT_TIMESTAMP() AS LOAD_TS_UTC,
                PARSE_JSON($1) AS PAYLOAD
              FROM @%%EXTERNAL_LOYALTY_EVENTS_RAW
            )
            FILE_FORMAT = (TYPE = 'JSON')
            """
            cur.execute(copy_sql, (ingestion_id, source))

            # COPY INTO returns per-file stats; sum rows_loaded
            rows = cur.fetchall() or []
            rows_loaded = 0
            for r in rows:
                # often: (file, status, rows_parsed, rows_loaded, errors, ...)
                if len(r) >= 4 and r[3] is not None:
                    try:
                        rows_loaded += int(r[3])
                    except Exception:
                        pass

        conn.commit()
        return ingestion_id, rows_loaded
    finally:
        conn.close()


# ============================================================
# 2) LOAD_STATE: watermark + last run metadata
# ============================================================
def ensure_load_state_table_exists(database: Optional[str] = None, schema: Optional[str] = None) -> None:
    """
    Create <schema>.LOAD_STATE if missing.

    One row per (PIPELINE, SOURCE):
      - LAST_SUCCESS_TS_UTC is your watermark (advance on success).
      - LAST_STATUS / LAST_MESSAGE help debugging.
    """
    database = _validate_identifier("database", database or _database())
    schema = _validate_identifier("schema", schema or _schema())

    ddl_table = f"""
    CREATE TABLE IF NOT EXISTS {schema}.LOAD_STATE (
      PIPELINE            STRING,
      SOURCE              STRING,
      LAST_SUCCESS_TS_UTC TIMESTAMP_NTZ,
      UPDATED_AT_UTC      TIMESTAMP_NTZ,
      LAST_RUN_ID         STRING,
      LAST_INGESTION_ID   STRING,
      LAST_ROWS_LOADED    NUMBER,
      LAST_STATUS         STRING,
      LAST_MESSAGE        STRING
    )
    """

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE DATABASE {database}")
            cur.execute(f"USE SCHEMA {schema}")
            cur.execute(ddl_table)
        conn.commit()
    finally:
        conn.close()


def get_last_success_ts(database: str, schema: str, pipeline: str, source: str) -> Optional[datetime]:
    """
    Read watermark: LAST_SUCCESS_TS_UTC for (pipeline, source).
    Returns UTC-aware datetime or None.
    """
    database = _validate_identifier("database", database)
    schema = _validate_identifier("schema", schema)

    ensure_load_state_table_exists(database=database, schema=schema)

    sql = f"""
    SELECT LAST_SUCCESS_TS_UTC
    FROM {schema}.LOAD_STATE
    WHERE PIPELINE = %s AND SOURCE = %s
    LIMIT 1
    """

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE DATABASE {database}")
            cur.execute(f"USE SCHEMA {schema}")
            cur.execute(sql, (pipeline, source))
            row = cur.fetchone()
            if not row or row[0] is None:
                return None

            # TIMESTAMP_NTZ comes back naive; treat as UTC
            return row[0].replace(tzinfo=timezone.utc)
    finally:
        conn.close()


def upsert_load_state(
    database: str,
    schema: str,
    pipeline: str,
    source: str,
    last_success_ts_utc: Optional[datetime],
    last_run_id: str,
    last_ingestion_id: str,
    last_rows_loaded: int,
    last_status: str,
    last_message: str,
) -> None:
    """
    Update/insert LOAD_STATE row.

    Watermark behavior:
      - On SUCCESS: pass a datetime -> watermark advances
      - On FAIL:    pass None -> watermark stays unchanged (COALESCE keeps previous)
    """
    database = _validate_identifier("database", database)
    schema = _validate_identifier("schema", schema)

    ensure_load_state_table_exists(database=database, schema=schema)

    now_ntz = datetime.now(timezone.utc).replace(tzinfo=None)
    ts_ntz = None
    if last_success_ts_utc is not None:
        ts_ntz = last_success_ts_utc.astimezone(timezone.utc).replace(tzinfo=None)

    merge_sql = f"""
    MERGE INTO {schema}.LOAD_STATE t
    USING (SELECT %s AS PIPELINE, %s AS SOURCE) s
      ON t.PIPELINE = s.PIPELINE AND t.SOURCE = s.SOURCE
    WHEN MATCHED THEN UPDATE SET
      LAST_SUCCESS_TS_UTC = COALESCE(%s, t.LAST_SUCCESS_TS_UTC),
      UPDATED_AT_UTC      = %s,
      LAST_RUN_ID         = %s,
      LAST_INGESTION_ID   = %s,
      LAST_ROWS_LOADED    = %s,
      LAST_STATUS         = %s,
      LAST_MESSAGE        = %s
    WHEN NOT MATCHED THEN INSERT (
      PIPELINE, SOURCE, LAST_SUCCESS_TS_UTC, UPDATED_AT_UTC,
      LAST_RUN_ID, LAST_INGESTION_ID, LAST_ROWS_LOADED, LAST_STATUS, LAST_MESSAGE
    ) VALUES (
      %s, %s, %s, %s,
      %s, %s, %s, %s, %s
    )
    """

    params = (
        pipeline,
        source,
        ts_ntz,
        now_ntz,
        last_run_id,
        last_ingestion_id,
        last_rows_loaded,
        last_status,
        last_message,
        # insert values
        pipeline,
        source,
        ts_ntz,
        now_ntz,
        last_run_id,
        last_ingestion_id,
        last_rows_loaded,
        last_status,
        last_message,
    )

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE DATABASE {database}")
            cur.execute(f"USE SCHEMA {schema}")
            cur.execute(merge_sql, params)
        conn.commit()
    finally:
        conn.close()



