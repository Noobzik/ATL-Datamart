import os
import urllib.request
import datetime
from minio import Minio
import sys

def main():
    grab_data()
    grab_latest_month()
    write_data_minio()

def grab_data():
    """Grab data from January 2023 to August 2023."""
    folder_path = os.path.join(os.getcwd(), 'data', 'raw')
    os.makedirs(folder_path, exist_ok=True)
    data_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023'
    for month in range(1, 9):
        month_str = str(month).zfill(2)
        file_url = f'{data_url}-{month_str}.parquet'
        file_path = os.path.join(folder_path, f'2023-{month_str}-tripdata.parquet')
        try:
            urllib.request.urlretrieve(file_url, file_path)
            print(f'Downloaded: {file_path}')
        except urllib.error.HTTPError as e:
            print(f"Error downloading {file_url}: {e}")

def grab_latest_month():
    """Grab data for January 2024."""
    folder_path = os.path.join(os.getcwd(), 'data', 'raw')
    os.makedirs(folder_path, exist_ok=True)
    target_year = 2024
    target_month = 1
    data_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{target_year}-{str(target_month).zfill(2)}.parquet'
    file_path = os.path.join(folder_path, f'{target_year}-{str(target_month).zfill(2)}-tripdata.parquet')
    try:
        urllib.request.urlretrieve(data_url, file_path)
        print(f'Downloaded: {file_path}')
    except urllib.error.HTTPError as e:
        print(f"Error downloading {data_url}: {e}")

def write_data_minio():
    """Upload downloaded files to Minio."""
    client = Minio("localhost:9000", access_key="minio", secret_key="minio123", secure=False)
    bucket_name = "datalake"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    else:
        print("Bucket already exists")
    folder_path = os.path.join(os.getcwd(), 'data', 'raw')
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            client.fput_object(bucket_name, filename, file_path)
            print(f"Uploaded to Minio: {filename}")

if __name__ == '__main__':
    sys.exit(main())
