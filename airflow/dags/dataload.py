from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd
import psycopg2
import os

# ------------------- CONFIGURATION -------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_BUCKET_NAME = "yellow-taxi-data"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

POSTGRES_HOST = "data-warehouse"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"

MONTHS = ["2024-10", "2024-11", "2024-12"]

# ------------------- Téléchargement depuis MinIO -------------------
def download_from_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    for month in MONTHS:
        filename = f"yellow_tripdata_{month}.parquet"
        local_file = f"/tmp/{filename}"

        client.fget_object(MINIO_BUCKET_NAME, filename, local_file)
        print(f"✅ Fichier téléchargé depuis MinIO : {local_file}")

# ------------------- Insertion dans PostgreSQL -------------------
def insert_into_postgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cur = conn.cursor()

    # Création table simple
    cur.execute("""
        CREATE TABLE IF NOT EXISTS yellow_taxi_data (
            month TEXT,
            tripdata TEXT
        );
    """)
    conn.commit()

    for month in MONTHS:
        local_file = f"/tmp/yellow_tripdata_{month}.parquet"

        with open(local_file, "rb") as file:
            content = file.read()

        cur.execute(
            "INSERT INTO yellow_taxi_data (month, tripdata) VALUES (%s, %s)",
            (month, content)
        )
        conn.commit()
        os.remove(local_file)
        print(f"✅ Fichier {local_file} inséré en base et supprimé.")

    cur.close()
    conn.close()

# ------------------- DAG -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "datamart_load_from_minio",
    default_args=default_args,
    description="Charger les données MinIO dans PostgreSQL Data Warehouse",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["minio", "postgres"],
) as dag:

    t1 = PythonOperator(
        task_id="download_files_from_minio",
        python_callable=download_from_minio
    )

    t2 = PythonOperator(
        task_id="insert_files_into_postgres",
        python_callable=insert_into_postgres
    )

    t1 >> t2
