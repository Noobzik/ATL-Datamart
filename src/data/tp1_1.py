import os
import pandas as pd
import requests
from minio import Minio
from datetime import datetime, timedelta
import tempfile
import psycopg2
import io

minio_client = Minio('localhost:9000',
                     access_key='minio',
                     secret_key='minio123',
                     secure=False)

bucket_name = 'datalake'
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

pg_conn = psycopg2.connect(host='localhost', port=5432, dbname='datalake', user='postgres', password='postgres')

cursor = pg_conn.cursor()

def download_and_upload_to_minio(start_month, end_month, bucket_name):
    current_month = start_month
    while current_month <= end_month:
        file_name = f"yellow_tripdata_{current_month.year}-{current_month.month:02d}.parquet"
        file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                file_path = temp_file.name
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
            
            minio_client.fput_object(bucket_name, file_name, file_path)
            print(f'Uploaded {file_name} to MinIO bucket {bucket_name}')
            
            df = pd.read_parquet(file_path)

            if 'passenger_count' in df.columns:
                df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)

            if 'RatecodeID' in df.columns:
                df['RatecodeID'] = df['RatecodeID'].fillna(0).astype(int)
            
            output = io.StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            
            cursor.copy_from(output, 'yellow_taxi_data', null='', columns=(list(df.columns)))
            pg_conn.commit()
            print(f'Loaded {file_name} into PostgreSQL database.')
            
            os.remove(file_path)
        else:
            print(f'Failed to download data for {current_month.year}-{current_month.month:02d}')
        
        current_month += timedelta(days=31)
        current_month = current_month.replace(day=1)

download_and_upload_to_minio(datetime(2023, 1, 1), datetime(2023, 8, 1), bucket_name)

cursor.close()
pg_conn.close()
