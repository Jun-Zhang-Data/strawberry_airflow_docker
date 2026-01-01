from __future__ import annotations

import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import snowflake.connector


def _conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE_AIRFLOW") or os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema="RAW",
    )



DDL = [
    """
    CREATE TABLE IF NOT EXISTS PMS_RAW (
      RECORD        VARIANT,
      SRC_FILE_NAME STRING,
      LOAD_TS_UTC   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS SURVEY_RAW (
      RECORD        VARIANT,
      SRC_FILE_NAME STRING,
      LOAD_TS_UTC   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS MEMBERSHIP_RAW (
      ID            STRING,
      FIRST_NAME    STRING,
      LAST_NAME     STRING,
      STATUS        STRING,
      IS_ACTIVE     BOOLEAN,
      LOAD_TS_UTC   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
      SRC_FILE_NAME STRING
    )
    """,
]


def ensure_tables():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
    finally:
        conn.close()


def seed_if_empty():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            # PMS
            cur.execute("SELECT COUNT(*) FROM PMS_RAW")
            if cur.fetchone()[0] == 0:
                cur.execute(
                    """
                    INSERT INTO PMS_RAW (RECORD, SRC_FILE_NAME)
                    SELECT
                      PARSE_JSON('{
                        "ID": 1,
                        "Reservation_no": "RES-001",
                        "Reservation_date": "2025-12-27",
                        "Updated_time_utc": "2025-12-27T08:00:00",
                        "Member_id": "M001",
                        "Hotel_id": 101,
                        "Booking_start_date": "2025-12-27",
                        "Booking_end_date": "2025-12-28",
                        "Status_code": "BOOKED",
                        "Room_rate": 1200.00,
                        "Total_amount_gross": 1200.00
                      }'),
                      'pms_seed.json'
                    """
                )

            # SURVEY
            cur.execute("SELECT COUNT(*) FROM SURVEY_RAW")
            if cur.fetchone()[0] == 0:
                cur.execute(
                    """
                    INSERT INTO SURVEY_RAW (RECORD, SRC_FILE_NAME)
                    SELECT
                      PARSE_JSON('{
                        "ID": 10,
                        "Member_id": "M001",
                        "Is_anonymous": false,
                        "Submitted_on_utc": "2025-12-27T10:00:00",
                        "Reservation_no": "RES-001",
                        "Hotel_id": 101
                      }'),
                      'survey_seed.json'
                    """
                )

            # MEMBERSHIP
            cur.execute("SELECT COUNT(*) FROM MEMBERSHIP_RAW")
            if cur.fetchone()[0] == 0:
                cur.execute(
                    """
                    INSERT INTO MEMBERSHIP_RAW (ID, FIRST_NAME, LAST_NAME, STATUS, IS_ACTIVE, SRC_FILE_NAME)
                    VALUES ('M001', 'Test', 'User', 'GOLD', TRUE, 'membership_seed.csv')
                    """
                )

        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="seed_raw_sources",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["setup", "raw"],
) as dag:
    t1 = PythonOperator(task_id="ensure_raw_tables", python_callable=ensure_tables)
    t2 = PythonOperator(task_id="seed_demo_rows_if_empty", python_callable=seed_if_empty)

    t1 >> t2
