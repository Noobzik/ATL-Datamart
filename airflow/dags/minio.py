from urllib import request
from minio import Minio
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error
import gc
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

MINIO_ENDPOINT = "http://minio:9000/"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET_NAME = "taxi"
OBJECT_NAME = "yellow_tripdata_2025-03.parquet"
S3_URL = f"s3://{BUCKET_NAME}/{OBJECT_NAME}"

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "data-warehouse",  # Utiliser le nom du service Docker
        "dbms_port": "5432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
    except Exception as e:
        success = False
        print(f"Error connection to the database: {e}")
    return success

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def exportToSql() -> None:
    df = pd.read_parquet(
        S3_URL,
        storage_options={
            "key": ACCESS_KEY,
            "secret": SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        }
    )
    parquet_df = clean_column_name(df)
    if not write_data_postgres(parquet_df):
        del parquet_df
        gc.collect()
        return
    del parquet_df
    gc.collect()

def download_parquet(**kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename = "yellow_tripdata"
    extension = ".parquet"
    month = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_url = f"{url}{filename}_{month}{extension}"
    file_path = f"/tmp/{filename}_{month}{extension}"

    try:
        request.urlretrieve(file_url, file_path)
        print(f"File downloaded to {file_path}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file: {str(e)}") from e

def upload_file(**kwargs):
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = 'taxi'
    month = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_path = f"/tmp/yellow_tripdata_{month}.parquet"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    client.fput_object(
        bucket_name=bucket,
        object_name=f"yellow_tripdata_{month}.parquet",
        file_path=file_path
    )
    os.remove(file_path)

def execute_sql_script_from_file(file_path):
    # Lire le script SQL depuis le fichier
    with open(file_path, 'r') as file:
        sql_script = file.read()

    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        host="data-mart",  # Utiliser le nom du service Docker
        database="data_mart",
        user="postgres",
        password="admin"
    )

    try:
        # Créer un curseur
        cur = conn.cursor()

        # Exécuter le script SQL
        cur.execute(sql_script)

        # Valider la transaction
        conn.commit()

        print("Script SQL exécuté avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'exécution du script SQL: {e}")
    finally:
        # Fermer la connexion
        if conn is not None:
            conn.close()

with DAG(dag_id='Grab_NYC_Data_to_Minio',
         start_date=days_ago(1),
         schedule_interval='0 0 1 * *',
         catchup=False,
         tags=['minio/read/write'],
         ) as dag:

    t1 = PythonOperator(
        task_id='download_parquet',
        python_callable=download_parquet
    )

    t2 = PythonOperator(
        task_id='upload_file_task',
        python_callable=upload_file
    )

    t3 = PythonOperator(
        task_id='export_to_data_warehouse',
        python_callable=exportToSql
    )

    t4 = PythonOperator(
        task_id='execute_sql_script',
        python_callable=execute_sql_script_from_file,
        op_kwargs={'file_path': '/opt/airflow/dags/insertion.sql'}
    )

    t1 >> t2 >> t4
