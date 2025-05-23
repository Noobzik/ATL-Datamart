from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

# MinIO config
MINIO_ENDPOINT = "minio:9000"
MINIO_BUCKET_NAME = "yellow-taxi-data"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

# PostgreSQL config (staging db)
POSTGRES_CONN_STR = "postgresql://admin:admin@data-warehouse:5432/staging"

# Fichier à récupérer
PARQUET_FILE = "yellow_tripdata_2024-01.parquet"
LOCAL_FILE_PATH = f"/tmp/{PARQUET_FILE}"

def download_from_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    client.fget_object(MINIO_BUCKET_NAME, PARQUET_FILE, LOCAL_FILE_PATH)
    print("✅ Téléchargement de MinIO terminé.")

def load_to_postgres():
    df = pd.read_parquet(LOCAL_FILE_PATH)
    print(f"✅ Lecture du fichier Parquet OK - {len(df)} lignes")

    engine = create_engine(POSTGRES_CONN_STR)
    
    df.to_sql("stg_yellow_taxi", con=engine, if_exists='replace', index=False)
    print("✅ Données envoyées dans PostgreSQL (staging)")

    os.remove(LOCAL_FILE_PATH)
    print("🧹 Fichier local supprimé.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
}

with DAG(
    dag_id="datalake_to_staging",
    default_args=default_args,
    description="Charger données MinIO vers Postgres Staging",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["minio", "postgres", "staging"],
) as dag:

    t1 = PythonOperator(
        task_id="download_minio_file",
        python_callable=download_from_minio
    )

    t2 = PythonOperator(
        task_id="load_postgres_staging",
        python_callable=load_to_postgres
    )

    t1 >> t2
