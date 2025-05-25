# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio
import pendulum
import urllib.error
from airflow.utils.dates import days_ago
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator


def download_parquet(**kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    month = pendulum.now().subtract(months=2).format('YYYY-MM')
    filename = f"yellow_tripdata_{month}.parquet"
    full_url = f"{url}{filename}"
    print(f"[green]Downloading {filename} from {full_url}")
    try:
        request.urlretrieve(full_url, filename)
        print(f"[green]Downloaded {filename} from {full_url}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e


def upload_file(**kwargs):
    import os
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = 'my-bucket'
    month = pendulum.now().subtract(months=2).format('YYYY-MM')
    filename = f"yellow_tripdata_{month}.parquet"
    # Ensure bucket exists
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    # Upload file
    client.fput_object(
        bucket_name=bucket,
        object_name=filename,
        file_path=filename
    )
    print(f"[green]Uploaded {filename} to Minio bucket {bucket}")
    # Remove local file
    if os.path.exists(filename):
        os.remove(filename)
        print(f"[yellow]Removed local file {filename}")


with DAG(
    dag_id='Grab_NYC_Data_to_Minio',
    start_date=days_ago(1),
    schedule_interval=None,
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
    t1 >> t2
