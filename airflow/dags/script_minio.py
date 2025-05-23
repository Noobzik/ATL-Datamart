# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import os
import urllib.error

def download_parquet(**kwargs):
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"

    month: str = pendulum.now().subtract(months=1).format('YYYY-MM')
    full_filename: str = f"{filename}_{month}{extension}"
    full_url: str = f"{url}{full_filename}"

    try:
        print(f"Downloading from: {full_url}")
        request.urlretrieve(full_url, full_filename)
        print(f"Saved to: {full_filename}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file: {str(e)}") from e


def upload_file(**kwargs):
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    bucket = 'rawnyc'

    month = pendulum.now().subtract(months=1).format('YYYY-MM')
    filename = f"yellow_tripdata_{month}.parquet"

    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")
    else:
        print(f"Bucket '{bucket}' already exists.")

    client.fput_object(bucket_name=bucket,
                       object_name=filename,
                       file_path=filename)
    print(f"Uploaded {filename} to bucket '{bucket}'")

    os.remove(filename)
    print(f"Removed local file: {filename}")

default_args = {
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
}

with DAG(dag_id='grab_nyc_data_to_minio',
         schedule=None,
         start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
         catchup=False,
         tags=['minio', 'nyc', 'parquet']) as dag:

    t1 = PythonOperator(
        task_id='download_parquet',
        python_callable=download_parquet
    )

    t2 = PythonOperator(
        task_id='upload_file_task',
        python_callable=upload_file
    )

    t1 >> t2

