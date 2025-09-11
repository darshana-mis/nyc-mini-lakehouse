from datetime import datetime
import os
from io import BytesIO, StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import requests
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import psycopg2

# ---------- Config ----------
DB_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "nyc",
    "user": "dbuser",
    "password": "dbpass",
}

DEFAULT_MONTH = "2023-01"  # will be overridden by dag_run.conf["month"] if provided

def _month_from_conf(context) -> str:
    """Allow month to be overridden when triggering the DAG."""
    conf = (context.get("dag_run") or {}).conf or {}
    month = conf.get("month", DEFAULT_MONTH)
    # minimal sanity check YYYY-MM
    if isinstance(month, str) and len(month) == 7 and month[4] == "-":
        return month
    return DEFAULT_MONTH

def download_parquet(**context):
    """
    Download TLC Yellow parquet for the selected month and save it locally.
    Push the local parquet path to XCom.
    """
    month = _month_from_conf(context)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    # Read parquet bytes into an Arrow table to validate file integrity
    table = pq.read_table(BytesIO(resp.content))

    os.makedirs("/opt/airflow/tmp", exist_ok=True)
    out_path = f"/opt/airflow/tmp/yellow_{month}.parquet"
    pq.write_table(table, out_path)

    ti = context["ti"]
    ti.xcom_push(key="parquet_path", value=out_path)
    ti.xcom_push(key="month", value=month)

def load_to_postgres(**context):
    """
    Read parquet from XCom path, create staging table if not exists,
    truncate it, and bulk load rows via COPY for speed.
    """
    ti = context["ti"]
    parquet_path = ti.xcom_pull(key="parquet_path", task_ids="download")
    month = ti.xcom_pull(key="month", task_ids="download") or DEFAULT_MONTH

    # Read parquet → pandas
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    # Keep only the columns we model
    cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "PULocationID",
        "DOLocationID",
    ]
    # Some months may have slight schema drift; guard against missing cols
    present = [c for c in cols if c in df.columns]
    df = df[present].copy()

    # Coerce dtypes lightly to reduce COPY errors
    int_cols = ["VendorID", "passenger_count", "payment_type", "PULocationID", "DOLocationID"]
    float_cols = ["trip_distance", "fare_amount", "tip_amount", "total_amount"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    create_sql = """
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE TABLE IF NOT EXISTS staging.yellow_trips (
        VendorID INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance DOUBLE PRECISION,
        fare_amount DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        payment_type INTEGER,
        PULocationID INTEGER,
        DOLocationID INTEGER
    );
    TRUNCATE TABLE staging.yellow_trips;
    """

    # Use COPY for fast bulk load
    with psycopg2.connect(**DB_CONN) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(create_sql)

            # Convert DataFrame to CSV in-memory (no header)
            buf = StringIO()
            df.to_csv(buf, index=False, header=False)
            buf.seek(0)

            copy_sql = f"COPY staging.yellow_trips({', '.join(present)}) FROM STDIN WITH CSV"
            cur.copy_expert(copy_sql, buf)

def month_info(**context):
    # Helpful no-op that logs which month ran (shows in UI)
    month = _month_from_conf(context)
    print(f"Running for month: {month}")

# ---------- DAG ----------
with DAG(
    dag_id="nyc_yellow_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc", "taxi", "elt"],
) as dag:

    t0 = PythonOperator(task_id="month_info", python_callable=month_info)
    t1 = PythonOperator(task_id="download", python_callable=download_parquet)
    t2 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    # dbt models (already mounted at /opt/airflow/dbt)
    t3 = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run && dbt test",
    )

    t0 >> t1 >> t2 >> t3
