"""
Seed RAW source tables in Snowflake with small demo data.

This DAG is for local/dev so you can run dbt without building real ingestion yet.

It:
- Uses an existing Snowflake database (SNOWFLAKE_DATABASE)
- Creates schema (SNOWFLAKE_SCHEMA, default RAW) if allowed
- Creates RAW tables if missing:
    PMS_RAW        (RECORD VARIANT)
    SURVEY_RAW     (RECORD VARIANT)  <-- includes question_*/answer_* keys
    MEMBERSHIP_RAW (typed columns)
- Deletes old seed rows (by SRC_FILE_NAME) and inserts fresh seed rows

Run:
- Trigger DAG `seed_raw_sources` in Airflow UI
- Then run dbt (snapshot/build)
"""

import os
from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "seed_raw_sources"
DEFAULT_ARGS = {"owner": "airflow", "retries": 0}


def _sf_connect():
    """
    Connect using env vars (from docker-compose/.env).

    Required:
      SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    Optional:
      SNOWFLAKE_ROLE or SNOWFLAKE_ROLE_AIRFLOW
      SNOWFLAKE_WAREHOUSE
    """
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PASSWORD"]

    role = os.environ.get("SNOWFLAKE_ROLE") or os.environ.get("SNOWFLAKE_ROLE_AIRFLOW")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

    conn_kwargs = dict(
        account=account,
        user=user,
        password=password,
        client_session_keep_alive=False,
    )
    if role:
        conn_kwargs["role"] = role
    if warehouse:
        conn_kwargs["warehouse"] = warehouse

    return snowflake.connector.connect(**conn_kwargs)


def _exec_many(cur, statements):
    for sql in statements:
        cur.execute(sql)


def seed_raw_sources():
    db = os.environ.get("SNOWFLAKE_DATABASE", "").strip()
    raw_schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW").strip()

    if not db:
        raise RuntimeError("SNOWFLAKE_DATABASE is not set. Set it in .env / docker-compose env_file.")

    PMS_SEED_FILE = "pms_seed.json"
    SURVEY_SEED_FILE = "survey_seed.json"
    MEMBERSHIP_SEED_FILE = "membership_seed.csv"

    with _sf_connect() as conn:
        with conn.cursor() as cur:
            # Show context in logs (super helpful for debugging privileges)
            cur.execute("select current_user(), current_role()")
            user_role = cur.fetchone()
            print("Snowflake context (user, role):", user_role)
            print("Target database/schema:", db, raw_schema)

            # Use existing database; create schema if allowed
            create_db_and_schema = [
                f"USE DATABASE {db}",
                f"CREATE SCHEMA IF NOT EXISTS {db}.{raw_schema}",
                f"USE SCHEMA {db}.{raw_schema}",
            ]
            _exec_many(cur, create_db_and_schema)

            # Create tables if missing
            create_tables = [
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
            _exec_many(cur, create_tables)

            # Delete previous seed rows (keeps re-runs clean)
            delete_old_seed_rows = [
                f"DELETE FROM PMS_RAW WHERE SRC_FILE_NAME = '{PMS_SEED_FILE}'",
                f"DELETE FROM SURVEY_RAW WHERE SRC_FILE_NAME = '{SURVEY_SEED_FILE}'",
                f"DELETE FROM MEMBERSHIP_RAW WHERE SRC_FILE_NAME = '{MEMBERSHIP_SEED_FILE}'",
            ]
            _exec_many(cur, delete_old_seed_rows)

            # Insert PMS seed rows (IMPORTANT: has 'ID' numeric key for reservation_id)
            seed_pms = [
                f"""
                INSERT INTO PMS_RAW (RECORD, SRC_FILE_NAME)
                SELECT
                  PARSE_JSON('{{
                    "ID": 1,
                    "Reservation_no": "RES-001",
                    "Reservation_date": "2025-12-27",
                    "Updated_time_utc": "2025-12-27T09:15:00",
                    "Member_id": "M001",
                    "Hotel_id": 101,
                    "Booking_start_date": "2025-12-27",
                    "Booking_end_date": "2025-12-29",
                    "Status_code": "CONFIRMED",
                    "Room_rate": 999.00,
                    "Total_amount_gross": 1998.00
                  }}'),
                  '{PMS_SEED_FILE}'
                """,
                f"""
                INSERT INTO PMS_RAW (RECORD, SRC_FILE_NAME)
                SELECT
                  PARSE_JSON('{{
                    "ID": 2,
                    "Reservation_no": "RES-002",
                    "Reservation_date": "2025-12-28",
                    "Updated_time_utc": "2025-12-28T12:00:00",
                    "Member_id": "M002",
                    "Hotel_id": 101,
                    "Booking_start_date": "2025-12-28",
                    "Booking_end_date": "2025-12-30",
                    "Status_code": "CONFIRMED",
                    "Room_rate": 799.00,
                    "Total_amount_gross": 1598.00
                  }}'),
                  '{PMS_SEED_FILE}'
                """,
            ]
            _exec_many(cur, seed_pms)

            # Insert Survey seed row with question_*/answer_* keys (so your FLATTEN model produces rows)
            seed_survey = [
                f"""
                INSERT INTO SURVEY_RAW (RECORD, SRC_FILE_NAME)
                SELECT
                  PARSE_JSON('{{
                    "ID": 10,
                    "Member_id": "M001",
                    "Is_anonymous": false,
                    "Submitted_on_utc": "2025-12-27T10:00:00",
                    "Reservation_no": "RES-001",
                    "Hotel_id": 101,

                    "question_1": "How was your stay?",
                    "answer_1": 5,

                    "question_2": "Would you recommend us?",
                    "answer_2": true,

                    "question_3": "What did you like most?",
                    "answer_3": "Breakfast and friendly staff.",

                    "question_4": "Which facilities did you use?",
                    "answer_4": ["gym", "spa"],

                    "question_5": "Additional details",
                    "answer_5": {{"note": "Late check-in", "rating": 4}}
                  }}'),
                  '{SURVEY_SEED_FILE}'
                """
            ]
            _exec_many(cur, seed_survey)

            # Insert Membership seed rows (tier statuses to satisfy accepted_values tests)
            seed_membership = [
                f"""
                INSERT INTO MEMBERSHIP_RAW (ID, FIRST_NAME, LAST_NAME, STATUS, IS_ACTIVE, SRC_FILE_NAME)
                VALUES
                  ('M001', 'Alice',   'Andersson', 'BRONZE', TRUE,  '{MEMBERSHIP_SEED_FILE}'),
                  ('M002', 'Bob',     'Berg',      'SILVER', TRUE,  '{MEMBERSHIP_SEED_FILE}'),
                  ('M003', 'Cecilia', 'Carlsson',  'GOLD',   FALSE, '{MEMBERSHIP_SEED_FILE}')
                """
            ]
            _exec_many(cur, seed_membership)

            # Small sanity prints (shows counts after seed)
            cur.execute("select count(*) from PMS_RAW where src_file_name = %s", (PMS_SEED_FILE,))
            print("Seeded PMS_RAW rows:", cur.fetchone()[0])

            cur.execute("select count(*) from SURVEY_RAW where src_file_name = %s", (SURVEY_SEED_FILE,))
            print("Seeded SURVEY_RAW rows:", cur.fetchone()[0])

            cur.execute("select count(*) from MEMBERSHIP_RAW where src_file_name = %s", (MEMBERSHIP_SEED_FILE,))
            print("Seeded MEMBERSHIP_RAW rows:", cur.fetchone()[0])

    print("✅ Seed completed successfully.")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["seed", "snowflake", "raw"],
) as dag:
    PythonOperator(
        task_id="seed_raw_sources",
        python_callable=seed_raw_sources,
    )

