from datetime import datetime
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import pendulum
import os
import urllib.error


def download_parquet(**kwargs):
    # URL base + fichier à télécharger
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"

    # Récupération du mois actuel - 2
    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_url = f"{url}{filename}_{month}{extension}"
    local_filename = f"{filename}_{month}{extension}"

    try:
        print(f"Téléchargement depuis : {file_url}")
        request.urlretrieve(file_url, local_filename)
        print(f"Fichier téléchargé : {local_filename}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Erreur de téléchargement du fichier Parquet : {str(e)}") from e


def upload_file(**kwargs):
    # Connexion Minio
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = 'bucket'

    # Nom du fichier à uploader
    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    local_filename = f"yellow_tripdata_{month}.parquet"

    # Vérifie que le bucket existe, sinon le créer
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    # Upload
    print(f"Upload de {local_filename} vers MinIO bucket={bucket}")
    client.fput_object(
        bucket_name=bucket,
        object_name=local_filename,
        file_path=local_filename
    )

    # Suppression fichier local
    os.remove(local_filename)
    print(f"Fichier local supprimé : {local_filename}")


# DAG Airflow
with DAG(
    dag_id='grab_nyc_data_to_minio',
    schedule_interval="0 0 1 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['minio', 'parquet', 'nyc'],
) as dag:

    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )

    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
    
    trigger_tp2_dag = TriggerDagRunOperator(
        task_id="trigger_tp2_dump_to_sql",
        trigger_dag_id="tp2_dump_to_sql",  # ID of the second DAG
    )

    t1 >> t2 >> trigger_tp2_dag
