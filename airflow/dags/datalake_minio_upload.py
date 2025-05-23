from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import os
from minio import Minio
from minio.error import S3Error

# ------------------- CONFIGURATION -------------------
MINIO_ENDPOINT = "minio:9000"
MINIO_BUCKET_NAME = "yellow-taxi-data"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

# Liste des mois à télécharger
MONTHS = ["2024-10", "2024-11", "2024-12"]

# ------------------- Tâche 1 : Télécharger -------------------
def download_files():
    for month in MONTHS:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
        local_file_path = f"/tmp/yellow_tripdata_{month}.parquet"
        try:
            print(f"Téléchargement depuis {url}...")
            urllib.request.urlretrieve(url, local_file_path)
            print(f"✅ Fichier téléchargé : {local_file_path}")
        except Exception as e:
            raise RuntimeError(f"Erreur pendant le téléchargement : {str(e)}")

# ------------------- Tâche 2 : Upload vers Minio -------------------
def upload_files():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Vérifie si le bucket existe
    if not client.bucket_exists(MINIO_BUCKET_NAME):
        client.make_bucket(MINIO_BUCKET_NAME)
        print(f"✅ Bucket {MINIO_BUCKET_NAME} créé.")
    else:
        print(f"ℹ️ Bucket {MINIO_BUCKET_NAME} existe déjà.")

    for month in MONTHS:
        local_file_path = f"/tmp/yellow_tripdata_{month}.parquet"
        filename = os.path.basename(local_file_path)

        try:
            client.fput_object(MINIO_BUCKET_NAME, filename, local_file_path)
            print(f"✅ Fichier {filename} envoyé dans {MINIO_BUCKET_NAME}")
        except S3Error as e:
            raise RuntimeError(f"Erreur MinIO : {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Erreur : {str(e)}")
        finally:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                print(f"🧹 Fichier local supprimé : {local_file_path}")

# ------------------- DAG -------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "datalake_minio_upload_multiple",
    default_args=default_args,
    description="Télécharge et envoie plusieurs fichiers NYC vers MinIO",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["minio", "nyc"],
) as dag:

    t1 = PythonOperator(
        task_id="download_files",
        python_callable=download_files
    )

    t2 = PythonOperator(
        task_id="upload_files",
        python_callable=upload_files
    )

    t1 >> t2
