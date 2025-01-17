from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request
import os
from minio import Minio
from minio.error import S3Error

# Configuration de MinIO
MINIO_ENDPOINT = "minio:9000"  
MINIO_BUCKET_NAME = "yellow-taxi-data"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

# URL de téléchargement des données NYC
NYC_DATA_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Fonction pour télécharger les données
def download_parquet_file():
    local_file_path = "/tmp/yellow_tripdata_2024-01.parquet"
    try:
        print(f"Téléchargement du fichier depuis {NYC_DATA_URL}...")
        urllib.request.urlretrieve(NYC_DATA_URL, local_file_path)
        print(f"Fichier téléchargé avec succès : {local_file_path}")
    except Exception as e:
        raise RuntimeError(f"Erreur lors du téléchargement du fichier : {str(e)}")
    return local_file_path

# Fonction pour envoyer le fichier vers MinIO
def upload_to_minio(local_file_path):
    try:
        print("Connexion à MinIO...")
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        # Vérifier si le bucket existe, sinon le créer
        if not client.bucket_exists(MINIO_BUCKET_NAME):
            client.make_bucket(MINIO_BUCKET_NAME)
            print(f"Bucket {MINIO_BUCKET_NAME} créé.")
        else:
            print(f"Bucket {MINIO_BUCKET_NAME} déjà existant.")

        # Téléverser le fichier
        object_name = os.path.basename(local_file_path)
        client.fput_object(MINIO_BUCKET_NAME, object_name, local_file_path)
        print(f"Fichier téléversé avec succès : {object_name} dans le bucket {MINIO_BUCKET_NAME}")
    except ConnectionRefusedError:
        raise RuntimeError("Impossible de se connecter à MinIO. Vérifiez que MinIO est en cours d'exécution.")
    except S3Error as e:
        raise RuntimeError(f"Erreur MinIO : {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Erreur inconnue : {str(e)}")
    finally:
        # Supprimer le fichier local après l'envoi
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Fichier local supprimé : {local_file_path}")

# Définir le DAG et les tâches
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="grab_nyc_data_to_minio",
    default_args=default_args,
    description="Télécharge des données NYC et les stocke dans MinIO",
    schedule="0 12 * * *",  # Exécuter tous les jours à midi
    start_date=datetime(2024, 12, 15),
    catchup=False,
) as dag:

    # Tâche 1 : Télécharger le fichier Parquet
    download_task = PythonOperator(
        task_id="download_parquet_file",
        python_callable=download_parquet_file,
    )

    # Tâche 2 : Envoyer le fichier vers MinIO
    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=lambda: upload_to_minio(download_parquet_file()),
    )

    # Définir l'ordre des tâches
    download_task >> upload_task
