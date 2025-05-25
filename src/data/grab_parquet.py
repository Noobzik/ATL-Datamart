from minio import Minio
import urllib.request
from datetime import date
import pandas as pd
import os
import sys
from botocore.exceptions import NoCredentialsError
import boto3

MINIO_ENDPOINT = "http://127.0.0.1:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"

def main():
    file_name = "yellow_taxi_latest.parquet" 
    save_path = os.path.join( "data", "raw", file_name)
    grab_data(save_path)
    write_data_minio(save_path)


    
    

def grab_data(savePath) -> None:
    """Grab the data from New York Yellow Taxi

    This method downloads a Parquet file of the New York Yellow Taxi data for the latest available month.
    The file is saved into the "../../data/raw" folder.
    savePath - str - the path where the file will be downloaded on the machine 
    This method returns nothing.
    """
    # souce fichier novembre 2024
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Récupérer le mois et l'année actuelle
    current_date = date.today()
    year = current_date.year
    month = current_date.month - 4 

    if month <= 0:
        month += 12
        year -= 1
    
    month = f"{month:02d}"  
    
    file_url = base_url + f"yellow_tripdata_{year}-{month}.parquet"
    
    # Définir le dossier de destination
    
    os.makedirs(os.path.dirname(savePath), exist_ok=True)
    
    try:
        print(f"Téléchargement de {file_url}...")
        urllib.request.urlretrieve(file_url, savePath)
        print(f"Fichier téléchargé et sauvegardé sous {savePath}, remplacement effectué si nécessaire.")
    except Exception as e:
        print(f"Erreur lors du téléchargement du fichier: {e}")


def upload_to_minio(file_path, bucket_name, object_name=None):
    """Upload un fichier vers Minio."""
    s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"✅ Fichier {object_name} uploadé dans Minio ({bucket_name})")
    except NoCredentialsError:
        print("❌ Erreur : Credentials Minio non valides.")


def write_data_minio(filePath):
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    filePath - str - path where the file is located
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "taxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    
    upload_to_minio(filePath, "taxi",)

if __name__ == '__main__':
    sys.exit(main())

