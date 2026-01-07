
# Main Issue :
# ------------------------------------------------
# solve the SQL dead lock issue when deleting staging table, isolating by using run_id from the Airflow context.
# solve the Pandas datetime issue to load data from MongoDB to PostgreSQL.
#------------------------------------------------


"""
DISCLAIMER:
This DAG is a personal demonstration project.
All database names, collection names, schemas, and fields are anonymized.
No proprietary or production data is included.
"""


# --------------------------------------------------------
# MongoDB to PostgreSQL ETL-6 Tasks [ Backfill DAG Daily] 
# --------------------------------------------------------

import subprocess
import logging
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, timezone, datetime
import os
import json
import pandas as pd
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --------------------------------------------------
# Load ENV variables
# --------------------------------------------------
load_dotenv()

MONGO_URI = os.getenv("mongodb_conn_report_new")
MONGO_DB = "test_db"
MONGO_COLLECTION = "test_collection"

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = "5432"
PG_DB = "db_test"

# --------------------------------------------------
# Helper Functions
# --------------------------------------------------
def get_pg_conn():
    return psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

def clean_value(x):
    if isinstance(x, (dict, list)):
        return json.dumps(x, default=str)
    return x

# --------------------------------------------------
# Task 1: Check Connections
# --------------------------------------------------
def check_connections():
    MongoClient(MONGO_URI).admin.command("ping")
    conn = get_pg_conn()
    conn.cursor().execute("SELECT 1")
    conn.close()
    print("MongoDB & PostgreSQL connections OK")

# --------------------------------------------------
# Task 2: Extract Mongo → Staging (Backfill Safe)
# --------------------------------------------------
# (**context) this task must know which logical date window (Nov 1, Nov 2, …) it is responsible for, and Airflow passes that information through the task context.
# When Airflow runs a task, it automatically creates a runtime context containing metadata about that specific DAG run.
# This is NOT today’s date. This is NOT the system clock datetime.
#  start_date is coming from the DAG definition

def extract_mongo_to_staging(**context):
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    selected_columns = {
        "_id": 1,
        "address": 1,
        "country": 1,
        "createdAt": 1,
        "createdBy": 1,
        "email": 1,
        "name": 1,
        "phone": 1,
        "requestParams": 1,
        "settlement": 1,
        "stateChangedAt": 1,
        "status": 1,
        "statusChangedAt": 1,
        "statusChangedBy": 1,
        "type": 1,
        "updatedAt": 1,
        "updatedBy": 1
    }

    # DAG execution time window or logical date window
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    run_id = context["run_id"] # run_id is a unique identifier for the current DAG run

    # check both createdAt and updatedAt dates
    query = {
        "$or": [
            {"createdAt": {"$gte": data_interval_start, "$lt": data_interval_end}},
            {"updatedAt": {"$gte": data_interval_start, "$lt": data_interval_end}}
        ]
    }

    # sort by createdAt and _id
    docs = list(collection.find(query, selected_columns).sort([("createdAt", 1), ("_id", 1)]))
    df = pd.DataFrame(docs)

    # this df.empyty check is to avoid sending an empty df to the next task
    if df.empty:
        context["ti"].xcom_push(key="has_data", value=False)
        print("No new data for this window")
        return
    else:
        context["ti"].xcom_push(key="has_data", value=True) # Signals downstream tasks / “Yes, continue processing.”
        context["ti"].xcom_push(key="extracted_df", value=df.to_dict(orient="records"))
        print(f"Extracted {len(df)} records from MongoDB.")
    
    # This DAG uses XCom to pass extracted/transformed data between tasks for simplicity and local reproducibility. 
    # In production, large datasets MUST NOT be passed via XCom.
    # Instead, data would be: Written to an intermediate storage layer (S3 / GCS) or created as a temporary Json file on the local filesystem or
    # loaded into a SQL staging table and only a reference (path / batch_id) would be passed via XCom
      

# --------------------------------------------------
# Task 3: Transform
# --------------------------------------------------
def transform_task(**context):
    # In Airflow’s xcom_pull method, the correct parameter name is "task_ids" (plural), not task_id (singular).
    has_data = context["ti"].xcom_pull(task_ids="extract_mongo_to_staging", key="has_data")
    if not has_data:
        print("Skipping transform — no new data.")
        return

    records = context["ti"].xcom_pull(task_ids="extract_mongo_to_staging", key="extracted_df")
    df = pd.DataFrame(records)

    # Transforma "_id" to string to be ready for PostgreSQL
    df["_id"] = df["_id"].astype(str)
    df = normalize_columns(df)
    # CamelCase / Snake_case change to lower letters

    # handling nested fields from MongoDB to Json string to be ready for PostgreSQL as “one cell = one value” (not a structure, not a collection).
    for col in ["requestparams", "createdby", "updatedby", "statuschangedby"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_value)

    # handling datetime columns
    #  errors="coerce" means:If a value cannot be converted to a datetime, replace it with NaT instead of raising an error.
    datetime_cols = ["createdat", "updatedat", "statechangedat", "statuschangedat"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # CRITICAL FIX: NaT → None
    df = df.astype(object).where(pd.notnull(df), None) # check NaT and replace it with None as Psotgres does not accept NaT
    df["airflow_run_id"] = context["run_id"] 
    # run_id is a unique identifier for the current DAG run

    context["ti"].xcom_push(key="transformed_df", value=df.to_dict(orient="records")) # This makes the data XCom-safe.
    print(f"Transformed {len(df)} rows ready for load.")
    # convert df to a dictionary because Airflow XCom cannot reliably store pandas DataFrames,
    # but it can store JSON-serializable Python objects.

# --------------------------------------------------
# Task 4: Load into Final Mart
# --------------------------------------------------
def load_task(**context):
    transformed_records = context["ti"].xcom_pull(task_ids="transform_task", key="transformed_df")
    if not transformed_records:
        print("Skipping load — no data.")
        return

    df = pd.DataFrame(transformed_records)
    run_id = context["run_id"]
    # In Airflow, every DAG run has a unique identifier called run_id.
    # This ID tells how the DAG was triggered (manual / scheduled / backfill)
    # uniquely identifies this specific execution of the DAG

    conn = get_pg_conn()
    cursor = conn.cursor()

    # Ensure final mart exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.test_table_v3 (
            _id TEXT PRIMARY KEY,
            address TEXT,
            country TEXT,
            createdat TIMESTAMP,
            email TEXT,
            name TEXT,
            phone TEXT,
            settlement TEXT,
            statechangedat TIMESTAMP,
            status TEXT,
            statuschangedat TIMESTAMP,
            type TEXT,
            updatedat TIMESTAMP,
            requestparams JSONB,

            createdby_id TEXT,
            createdby_name TEXT,
            createdby_role TEXT,
            createdby_client TEXT,

            statuschangedby_id TEXT,
            statuschangedby_name TEXT,
            statuschangedby_role TEXT,
            statuschangedby_client TEXT,

            updatedby_id TEXT,
            updatedby_name TEXT,
            updatedby_role TEXT,
            updatedby_client TEXT,
            airflow_run_id TEXT      -- << airflow run id
        )
    """)

    # Insert data into staging table first
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.test_table_raw_v3 (
            _id TEXT,
            address TEXT,
            country TEXT,
            createdat TIMESTAMP,
            createdby JSONB,
            email TEXT,
            name TEXT,
            phone TEXT,
            requestparams JSONB,
            settlement TEXT,
            statechangedat TIMESTAMP,
            status TEXT,
            statuschangedat TIMESTAMP,
            statuschangedby JSONB,
            type TEXT,
            updatedat TIMESTAMP,
            updatedby JSONB,
            airflow_run_id TEXT
        )
    """)

    cols = list(df.columns) # this code is used to get the column names.
    values = [tuple(row) for row in df.to_numpy()]  # this code is used to convert the dataframe to a list of tuples
    
    # this code is used to insert the column names/ .join is used to join the column names with a comma
    # then the code is used to insert the data into the staging table
    insert_sql = f"INSERT INTO staging.test_table_raw_v3 ({','.join(cols)}) VALUES %s"   
    execute_values(cursor, insert_sql, values) 

    # Load into final mart with ON CONFLICT DO UPDATE SET (upsert concept)
    cursor.execute(f"""
        INSERT INTO public.test_table_v3
        SELECT
            _id, address, country, createdat, email,
             name, phone, settlement,
            statechangedat, status, statuschangedat, type, updatedat,
            requestparams,
            createdby ->> 'id',
            createdby ->> 'name',
            createdby ->> 'role',
            createdby ->> 'client',
            statuschangedby ->> 'id',
            statuschangedby ->> 'name',
            statuschangedby ->> 'role',
            statuschangedby ->> 'client',
            updatedby ->> 'id',
            updatedby ->> 'name',
            updatedby ->> 'role',
            updatedby ->> 'client',
            airflow_run_id
        FROM staging.test_table_raw_v3
        WHERE airflow_run_id = %s
        ON CONFLICT (_id) DO UPDATE SET
            address = EXCLUDED.address,
            country = EXCLUDED.country,
            updatedat = EXCLUDED.updatedat,
            requestparams = EXCLUDED.requestparams
    """, (run_id,))

    # Clean staging data
    # (run_id,) means the unique identifier for the current DAG run
    cursor.execute("DELETE FROM staging.test_table_raw_v3 WHERE airflow_run_id = %s", (run_id,))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows into final mart.")
        
    

# ------------------------
# Soda Quality Check Task
# ------------------------
def run_soda_quality_check(**context):
    """
    Run Soda scan for data quality checks after ETL load.
    Uses logging and AirflowException for error handling.
    """

    # tries to read run_id from the Airflow context, If it exists -> assigns it to run_id, otherwise assigns "unknown".
    run_id = context.get("run_id", "unknown")
    logging.info(f"Running Soda quality scan for Airflow run_id: {run_id}")

    # Run Soda scan as subprocess
    # subprocess.run() is Python’s way to run a shell command from Python.
    # This line is equivalent to running this in the terminal inside  Airflow container
    result = subprocess.run(
        ["soda", "scan", "-d","postgres_db",
         "-c","/opt/airflow/soda/configuration.yml", "/opt/airflow/soda/soda_biller_scan.yml"],
        capture_output=True,
        text=True
    )
    # -d = data source name (Must match the name defined in configuration.yml)
    # -c = path to Soda configuration file
    # capture_output=True: capture the output of the command (stdout and stderr)
    # text=True: converts output from bytes -> string

    logging.info("Soda scan stdout:\n%s", result.stdout)
    if result.stderr:
        logging.error("Soda scan stderr:\n%s", result.stderr)

    if result.returncode != 0:
        raise AirflowException("Data quality checks failed")
    
    logging.info("Data quality checks passed successfully")
    
    # Airflow provides: A logging framework/ Per-task log files/ Log persistence (UI, file, remote logs).
    # Airflow does NOT do: It does not 1.log what the Python function is doing/ 2.subprocess stdout & stderr automatically/ 3.not interpret success/failure context for you.
    # must still emit logs by using logging.
    # Data Quality Test : data freshness, valid data type, regex check, unique, and not missing fields check.


#---------------------------------------
# Row count reconciliation
#---------------------------------------
def row_count_reconciliation(**context):
    """
    Reconcile row counts between MongoDB and PostgreSQL after ETL load.
    Raises AirflowException if counts mismatch.
    """

    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    conn = get_pg_conn()
    cursor = conn.cursor()
    
    # tries to read run_id from the Airflow context, If it exists -> assigns it to run_id, otherwise assigns "unknown".
    run_id = context.get("run_id", "unknown")

    # Get MongoDB count for this interval or logical date window
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    mongo_query = {
        "$or": [
            {"createdAt": {"$gte": data_interval_start, "$lt": data_interval_end}},
            {"updatedAt": {"$gte": data_interval_start, "$lt": data_interval_end}}
        ]
    }
    mongo_count = collection.count_documents(mongo_query)

    #  Get Postgres count loaded in this interval or logical date window
    cursor.execute(
        "SELECT COUNT(*) FROM public.test_table_v3 WHERE airflow_run_id = %s", 
        (run_id,)
    )
    pg_count = cursor.fetchone()[0] # returns one row as a tuple : (123,).That’s how Python DB-API works (psycopg2, MySQL, etc.)
    # Get the first column value from the first row returned by the query.
    

    cursor.close()
    conn.close()

    print(f"MongoDB count: {mongo_count}")
    print(f"PostgreSQL count: {pg_count}")

    if mongo_count != pg_count:
        raise AirflowException(
            f"Row count mismatch! MongoDB: {mongo_count}, PostgreSQL: {pg_count}"
        )

    print("Row count reconciliation passed successfully")


# --------------------------------------------------
# DAG Definition
# --------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ETL_mongo_to_postgres_biller_schedule_qc",
    start_date=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
    schedule_interval="*/30 * * * *",
    catchup=False, # True will do backfill
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1),  # cap total runtime per dag run
    # If a DAG run takes longer than 1 hours from start to finish, Airflow will automatically mark the entire DAG run as FAILED.
    tags=["mongodb", "postgres", "backfill"]
) as dag:

    check_conn = PythonOperator(
        task_id="check_connections",
        python_callable=check_connections
    )

    extract_task = PythonOperator(
        task_id="extract_mongo_to_staging",
        python_callable=extract_mongo_to_staging
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task
    )
    
  
    soda_quality_check = PythonOperator(
    task_id="soda_quality_check",
    python_callable=run_soda_quality_check,  
    provide_context=True   # if you need Airflow context
    )
    
    row_count_check = PythonOperator(
    task_id="row_count_reconciliation",
    python_callable=row_count_reconciliation,
    provide_context=True
    )
    
    
    check_conn >> extract_task >> transform >> load >> soda_quality_check >> row_count_check

