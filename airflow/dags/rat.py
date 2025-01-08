from datetime import datetime, timedelta
import os
import urllib.request
import ssl
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

# Configuration MinIO
MINIO_ENDPOINT = "127.0.0.1:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "yellow-taxi-data"

# URL de base pour les fichiers Parquet Yellow Taxi
BASE_URL = "https://www.nyc.gov/assets/tlc/downloads/"


def get_last_month_parquet():
    """Calcule le nom du fichier Parquet pour le dernier mois."""
    today = datetime.now()
    last_month = today.replace(day=1) - timedelta(days=1)  # Dernier jour du mois précédent
    return f"yellow_tripdata_{last_month.year}-{str(last_month.month).zfill(2)}.parquet"


def download_parquet_file(file_name):
    """Télécharge le fichier Parquet correspondant."""
    file_url = f"{BASE_URL}{file_name}"
    local_temp_dir = "/tmp"
    local_file_path = os.path.join(local_temp_dir, file_name)

    os.makedirs(local_temp_dir, exist_ok=True)
    context = ssl._create_unverified_context()  # Désactiver la vérification SSL

    try:
        print(f"Téléchargement de {file_name} depuis {file_url}...")
        with urllib.request.urlopen(file_url, context=context) as response, open(local_file_path, "wb") as out_file:
            out_file.write(response.read())
        print(f"{file_name} téléchargé avec succès.")
        return local_file_path
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")
        raise


def upload_to_minio(local_file_path, file_name):
    """Upload du fichier Parquet téléchargé vers MinIO."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    try:
        client.fput_object(MINIO_BUCKET, file_name, local_file_path)
        print(f"Fichier {file_name} uploadé avec succès dans le bucket '{MINIO_BUCKET}'.")
    except Exception as e:
        print(f"Erreur lors de l'upload vers MinIO : {e}")
        raise


def process_last_month_data(**kwargs):
    """Pipeline complet : télécharger le fichier et l'uploader."""
    file_name = get_last_month_parquet()
    local_file_path = download_parquet_file(file_name)
    upload_to_minio(local_file_path, file_name)


# Définition de la DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "download_and_store_last_month_taxi_data",
    default_args=default_args,
    description="Télécharge le fichier Parquet du dernier mois et le stocke dans MinIO",
    schedule_interval="@monthly",  # Exécution automatique chaque mois
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    process_data_task = PythonOperator(
        task_id="process_last_month_data",
        python_callable=process_last_month_data,
        provide_context=True,
    )

    process_data_task
