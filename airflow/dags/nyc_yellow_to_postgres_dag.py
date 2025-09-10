from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import gzip
from io import BytesIO
import requests
import psycopg2

DB_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "nyc",
    "user": "dbuser",
    "password": "dbpass",
}

MONTH = "2025-07"
URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{MONTH}.parquet"

def download_csv(**context):
    resp = requests.get(URL, timeout=60)
    resp.raise_for_status()
    buf = BytesIO(resp.content)
    with gzip.open(buf, 'rt') as f:
        df = pd.read_csv(f)
    os.makedirs('/opt/airflow/tmp', exist_ok=True)
    out = f"/opt/airflow/tmp/yellow_{MONTH}.parquet"
    df.to_parquet(out, index=False)
    context['ti'].xcom_push(key='parquet_path', value=out)

def load_to_postgres(**context):
    import pyarrow.parquet as pq
    parquet_path = context['ti'].xcom_pull(key='parquet_path', task_ids='download')
    df = pq.read_table(parquet_path).to_pandas()

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

    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            # Simple load
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO staging.yellow_trips
                    (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                     trip_distance, fare_amount, tip_amount, total_amount, payment_type,
                     PULocationID, DOLocationID)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """, tuple(row.fillna(0)))

with DAG(
    dag_id="nyc_yellow_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc", "taxi", "elt"]
) as dag:

    t1 = PythonOperator(task_id='download', python_callable=download_csv)
    t2 = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)
    t3 = BashOperator(task_id='dbt_run', bash_command='cd /opt/airflow/dbt && dbt run && dbt test')

    t1 >> t2 >> t3
