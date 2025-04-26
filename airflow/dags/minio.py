# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error

def download_parquet(**kwargs):
    """Download the Yellow Taxi data file for N months ago."""
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename = "yellow_tripdata"
    extension = ".parquet"

    # Par défaut, on prend 2 mois en arrière
    months_back = kwargs.get('months_back', 2)  # <--- récupère le paramètre ou utilise 2
    month = pendulum.now().subtract(months=months_back).format('YYYY-MM')

    try:
        request.urlretrieve(
            url + filename + "_" + month + extension,
            os.path.join("./", f"yellow_tripdata_{month}.parquet")
        )
        print(f"✅ Fichier téléchargé : yellow_tripdata_{month}.parquet")
    except urllib.error.URLError as e:
        raise RuntimeError(f"❌ Échec du téléchargement du fichier parquet : {str(e)}") from e



def upload_file(**kwargs):
    """Upload the downloaded file to Minio."""
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = 'rawnyc'
 # Par défaut, on prend 2 mois en arrière
    months_back = kwargs.get('months_back', 2)
    month = pendulum.now().subtract(months=months_back).format('YYYY-MM')
    file_name = f"yellow_tripdata_{month}.parquet"
    file_path = os.path.join("./", file_name)

    
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"✅ Bucket '{bucket}' créé.")
    else:
        print(f"ℹ️ Bucket '{bucket}' existe déjà.")

   
    client.fput_object(
        bucket_name=bucket,
        object_name=file_name,
        file_path=file_path
    )
    print(f"✅ Fichier uploadé dans Minio : {file_name}")

    # Suppression du fichier local après upload
    os.remove(file_path)
    print(f"🗑️ Fichier local supprimé : {file_name}")


###############################################
# Définir le DAG
###############################################
with DAG(
    dag_id='grab_nyc_data_to_minio',
    start_date=days_ago(1),
    schedule_interval=None,  # Manuel pour les tests
    catchup=False,
    tags=['minio', 'nyc', 'data_pipeline'],
) as dag:
    ###############################################
    # Créer les tâches
    t1 = PythonOperator(
        task_id='download_parquet_task',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )

    ###############################################
    # Définir l'ordre des tâches
    t1 >> t2
###############################################
