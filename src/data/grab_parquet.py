from minio import Minio
import urllib.request
import pandas as pd
import sys
import os
import requests

def main():
    grab_data()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method downloads files of the New York Yellow Taxi from January 2023 to August 2023. 
    
    Files need to be saved into "../../data/raw" folder.
    This method takes no arguments and returns nothing.
    """
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month}.parquet"
        
    months = ['01', '02', '03', '04', '05', '06', '07', '08']
    
    target_dir = "../../data/raw"
    os.makedirs(target_dir, exist_ok=True)
    
    for month in months:
        file_name = f"yellow_tripdata_2023-{month}.parquet"
        url = base_url.format(month=month)
        destination_path = os.path.join(target_dir, file_name)
        
        print(f"Downloading {file_name}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(destination_path, 'wb') as f:
                for chunk in response.iter_content():
                    f.write(chunk)
            print(f"Saved to {destination_path}")
        else:
            print(f"Failed to download {file_name}. HTTP status code: {response.status_code}")
    


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
    bucket: str = "yellowtaxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
        
    for root, dirs, files in os.walk("../../data/raw"):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                print(f"Uploading {file_path} to Minio...")
                client.fput_object(bucket, file, file_path)
                print(f"Uploaded {file_path} to Minio")

if __name__ == '__main__':
    sys.exit(main())
