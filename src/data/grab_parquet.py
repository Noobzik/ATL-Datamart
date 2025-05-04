from minio.error import S3Error
import urllib.request
import pandas as pd
import sys
from minio import Minio
def main():
    write_data_minio()

    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # URL to the NYX taxis data Parquet file (replace this with the actual URL)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"  # Replace with the actual URL

    # Send a GET request to download the file
    save_path ="C:/Users/Mehdi/Desktop/DSI/ATL-Datamart-Mehdi/data/raw/yellow_tripdata_2025-01.parquet"

    # Check if the request was successful
    # Download the file using urllib
    try:
        urllib.request.urlretrieve(url, save_path)
        print(f"File successfully downloaded and saved to: {save_path}")
    except Exception as e:
        print(f"Failed to download file. Error: {str(e)}")

from minio import Minio
from minio.error import S3Error

def write_data_minio():
    """
    This method uploads a Parquet file to MinIO
    """
    # Initialize MinIO client
    client = Minio(
        "localhost:9000",  # MinIO server address
        secure=False,      # Disable SSL (ensure your MinIO is running without SSL)
        access_key="minio",  # MinIO access key
        secret_key="minio123"  # MinIO secret key
    )
    
    # The file to upload, change this path if needed
    source_file = "C:/Users/Mehdi/Desktop/DSI/ATL-Datamart-Mehdi/data/raw/yellow_tripdata_2025-01.parquet"

    # The destination bucket and filename on the MinIO server
    bucket_name = "ny.taxis"
    destination_file = "my-test-file.parquet"

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name, destination_file, source_file,
    )
    print(
        source_file, "successfully uploaded as object",
        destination_file, "to bucket", bucket_name,
    )




if __name__ == '__main__':
    sys.exit(main())
