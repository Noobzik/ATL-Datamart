from minio import Minio
import pandas as pd
import pyarrow.parquet as pq
import sys
import os

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # Directory where the Parquet file is located
    directory = "./data/raw/"
    
    # List all files in the directory
    files = os.listdir(directory)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Filter Parquet files
    parquet_files = [file for file in files if file.endswith('.parquet')]
    
    # Process each Parquet file
    for file in parquet_files:
        # Do something with the data, for example, upload to Minio
        write_data_minio(directory, file)


def write_data_minio(directory,file):
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
    bucket: str = "tp1"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

    #Envoi des fichier sur Minio
    try:
        # Open the Parquet file in binary read mode
        with open(os.path.join(directory, file), 'rb') as data_file:
            # Upload the Parquet file to Minio
            client.put_object(bucket, file, data_file, os.path.getsize(os.path.join(directory, file)))
        print(f"File {file} uploaded successfully to Minio.")
    except Exception as e:
        print(f"Failed to upload {file} to Minio: {e}")
        
if __name__ == '__main__':
    sys.exit(main())
