import os
import pandas as pd
from io import BytesIO
from minio import Minio
import pendulum
from urllib import request
import urllib.error
from airflow.utils.dates import days_ago
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

BUCKET = 'my-bucket'
MINIO_KWARGS = dict(
    endpoint="minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

def get_month_filename():
    month = pendulum.now().subtract(months=2).format('YYYY-MM')
    return f"yellow_tripdata_{month}.parquet"

def download_parquet(**kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename = get_month_filename()
    full_url = f"{url}{filename}"
    print(f"[green]Downloading {filename} from {full_url}")
    try:
        request.urlretrieve(full_url, filename)
        print(f"[green]Downloaded {filename} from {full_url}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e

def upload_file(**kwargs):
    client = Minio(**MINIO_KWARGS)
    filename = get_month_filename()
    # Ensure bucket exists
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    # Upload file
    client.fput_object(
        bucket_name=BUCKET,
        object_name=filename,
        file_path=filename
    )
    print(f"[green]Uploaded {filename} to Minio bucket {BUCKET}")
    # Remove local file
    if os.path.exists(filename):
        os.remove(filename)
        print(f"[yellow]Removed local file {filename}")

with DAG(
    dag_id='ATL-Load-To-Minio',
    dag_display_name='ATL Load To Minio',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
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
