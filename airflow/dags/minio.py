# Import Python dependencies needed for the workflow
from urllib.request import urlretrieve
from minio import Minio
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
from datetime import timedelta
import pendulum

# Voici les fonctions adaptées pour une exécution avec Airflow
def download_parquet(**kwargs):
    """Function to download the latest month's data as a Parquet file."""
    current_date = pendulum.now()
    last_month = current_date.subtract(months=1)
    folder_path = os.path.join(os.getcwd(), 'data', 'raw')
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"yellow_tripdata_{last_month.format('YYYY-MM')}.parquet"
    file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    file_path = os.path.join(folder_path, file_name)
    
    try:
        urlretrieve(file_url, file_path)
        print(f"Downloaded: {file_path}")
    except Exception as e:
        print(f"Error downloading {file_url}: {e}")

def upload_file(**kwargs):
    """Function to upload a file to Minio."""
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    
    bucket_name = "datalake"
    folder_path = os.path.join(os.getcwd(), 'data', 'raw')
    last_month = pendulum.now().subtract(months=1)
    file_name = f"yellow_tripdata_{last_month.format('YYYY-MM')}.parquet"
    file_path = os.path.join(folder_path, file_name)
    
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    client.fput_object(bucket_name, file_name, file_path)
    print(f"Uploaded {file_name} to bucket {bucket_name}")
    
    # Optionally, remove the local file to clean up
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Removed the local file: {file_path}")

# DAG definition
with DAG(
    dag_id='Grab_NYC_Data_to_Minio',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='A DAG to download NYC taxi data and upload to Minio',
    schedule_interval='@monthly',  # This will schedule the DAG to run monthly
    start_date=days_ago(1),
    catchup=False,
    tags=['minio', 'nyc-taxi-data'],
) as dag:

    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet,
    )

    t2 = PythonOperator(
        task_id='upload_file_to_minio',
        provide_context=True,
        python_callable=upload_file,
    )

    t1 >> t2  # t1 will run before t2
