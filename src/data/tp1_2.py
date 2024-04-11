import os
import pandas as pd
import requests
from minio import Minio
import tempfile
import psycopg2
import io

def create_database():
    conn = psycopg2.connect(host='localhost', dbname='postgres', user='postgres', password='postgres')
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'datalake'")
            if not cursor.fetchone():
                cursor.execute("CREATE DATABASE datalake")
                print("Database created successfully")
            else:
                print("Database already exists")
    finally:
        conn.close()

def connect_database():
    return psycopg2.connect(host='localhost', port=5432, dbname='datalake', user='postgres', password='postgres')

def create_table():
    with connect_database() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS yellow_taxi_data_last (
                    "VendorID" INT,
                    tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
                    tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
                    passenger_count INT,
                    trip_distance NUMERIC,
                    "RatecodeID" INT,
                    store_and_fwd_flag CHAR(1),
                    "PULocationID" INT,
                    "DOLocationID" INT,
                    payment_type INT,
                    fare_amount NUMERIC,
                    extra NUMERIC,
                    mta_tax NUMERIC,
                    tip_amount NUMERIC,
                    tolls_amount NUMERIC,
                    improvement_surcharge NUMERIC,
                    total_amount NUMERIC,
                    congestion_surcharge NUMERIC,
                    airport_fee NUMERIC DEFAULT NULL,
                    "Airport_fee" NUMERIC DEFAULT NULL
                );
            """)
            print("Table created successfully")
            conn.commit()

minio_client = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
bucket_name = 'datalakelast'
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

create_database()
create_table()

def download_and_upload_last_month_to_minio(year, month, bucket_name):
    with connect_database() as conn:
        with conn.cursor() as cursor:
            file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
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
                
                cursor.copy_from(output, 'yellow_taxi_data_last', null='', columns=(list(df.columns)))
                conn.commit()
                print(f'Loaded {file_name} into PostgreSQL database.')
                
                os.remove(file_path)
            else:
                print(f'Failed to download data for {year}-{month:02d}')

download_and_upload_last_month_to_minio(2023, 12, bucket_name)