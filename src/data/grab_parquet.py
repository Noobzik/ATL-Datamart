import requests
from minio import Minio
import urllib.request
import pandas as pd
import sys

def main():
    #get_all_data()
    get_last_month_data()
    
def get_last_month_data():
    grab_data()
    write_data_minio()
    
def get_all_data():
    grab_all_data()
    write_all_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    year = 2024
    month = 12
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month if month >= 10 else "0" + str(month)}.parquet" # fonctionne jusqu'a 2024-12, voir données dispo ici : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    response = requests.get(url)
    with open(f'./data/raw/yellow_tripdata_{year}-{month if month >= 10 else "0" + str(month)}.parquet', 'wb') as file:
        file.write(response.content)

def grab_all_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    for year in range(2009,2025):
        for month in range(1,13):
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month if month >= 10 else "0" + str(month)}.parquet"
            response = requests.get(url)
            with open(f'./data/raw/yellow_tripdata_{year}-{month}.parquet', 'wb') as file:
                file.write(response.content)
    


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "bucket"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    client.fput_object(bucket, "yellow_tripdata_2024-12.parquet", "./data/raw/yellow_tripdata_2024-12.parquet")

def write_all_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "bucket"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    for year in range(2009,2025):
        for month in range(1,13):
            client.fput_object(bucket, f"yellow_tripdata_{year}-{month if month >= 10 else "0" + str(month)}.parquet", f"./data/raw/yellow_tripdata_{year}-{month if month >= 10 else "0" + str(month)}.parquet")

if __name__ == '__main__':
    sys.exit(main())
