from minio import Minio
import urllib.request
import pandas as pdpip
import sys
import os


def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
months=["2024-10", "2024-11", "2024-12"]


base_url= "https://d37ci6vzurychx.cloudfront.net/trip-data/"
for month in months :
    filename = f"yellow_tripdata_{month}.parquet"
    url= base_url+filename
    output_path = f"../../data/raw/{filename}"
    print(f"Téléchargement de {filename}...")
    urllib.request.urlretrieve(url, output_path)


def write_data_minio():
    """Upload all Parquet files into Minio."""
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "nyc-yellow-taxi"  

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"✅ Bucket '{bucket}' créé.")
        else:
            print(f"ℹ️ Bucket '{bucket}' existe déjà.")
    except Exception as e:
        print(f"❌ Erreur lors de la vérification/création du bucket : {e}")
        return

    folder_path = "../../data/raw"
    for filename in os.listdir(folder_path):
        if filename.endswith(".parquet"):
            file_path = os.path.join(folder_path, filename)
            object_name = filename

            client.fput_object(bucket, object_name, file_path)
            print(f"Fichier {filename} uploadé dans Minio.")
            
if __name__ == '__main__':
    sys.exit(main())
