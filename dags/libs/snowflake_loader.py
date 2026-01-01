import os
from typing import List, Dict, Any

import snowflake.connector


def _get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE_AIRFLOW") or os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
    )




def ensure_external_loyalty_table_exists():
    """
    Create RAW.EXTERNAL_LOYALTY_EVENTS if it does not exist.
    Adjust database/schema if needed.
    """
    ddl = """
    create table if not exists RAW.EXTERNAL_LOYALTY_EVENTS (
        EVENT_ID        string,
        MEMBER_ID       string,
        EVENT_TYPE      string,
        POINTS          number,
        CREATED_AT      timestamp_tz,
        RAW_PAYLOAD     string,
        LOAD_TS_UTC     timestamp_tz default current_timestamp()
    )
    """

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        conn.close()


def insert_loyalty_events(rows: List[Dict[str, Any]]) -> int:
    """
    Insert normalized loyalty events into RAW.EXTERNAL_LOYALTY_EVENTS.

    Returns number of rows inserted.
    """
    if not rows:
        return 0

    insert_sql = """
        insert into RAW.EXTERNAL_LOYALTY_EVENTS
        (EVENT_ID, MEMBER_ID, EVENT_TYPE, POINTS, CREATED_AT, RAW_PAYLOAD)
        values (%(event_id)s, %(member_id)s, %(event_type)s, %(points)s, %(created_at)s, %(raw_payload)s)
    """

    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()
    finally:
        conn.close()

    return len(rows)
