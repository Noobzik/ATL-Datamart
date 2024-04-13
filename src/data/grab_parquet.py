from minio import Minio
import pandas as pd
import pyarrow.parquet as pq
from minio.error import S3Error
import sys
import os
import requests

def main():
    grab_data()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi"""

    # Créer le répertoire s'il n'existe pas
    directory = "./data/raw/"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Liste des mois à télécharger
    months = ['11', '12']  # Vous pouvez ajouter d'autres mois si nécessaire

    # Année à télécharger
    year = '2023'  # Modifier l'année si nécessaire

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"

    for month in months:
        url = f"{base_url}{year}-{month}.parquet"
        print(f"URL récupérer:  {url}")
        download_file(url, directory)

def download_file(url: str, directory: str) -> None:
    """Télécharger un fichier à partir de l'URL spécifiée"""

    file_name = url.split("/")[-1]  # Nom du fichier à partir de l'URL
    print(f"Téléchargement du fichier {file_name}...")
    response = requests.get(url)
    with open(os.path.join(directory, file_name), 'wb') as f:
        f.write(response.content)
    print(f"Fichier {file_name} téléchargé avec succès.")
    get_file_from_directory(directory)

def get_file_from_directory(directory: str) -> None:
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

def write_data_minio(directory, file):
    """Mettre tous les fichiers Parquet dans Minio"""

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "nyc-taxi-data"
    try:
        # Vérifiez si le bucket existe
        if client.bucket_exists(bucket):
            print(f"Le bucket '{bucket}' existe déjà.")
        # Si le bucket n'existe pas, créez-le
        else:
            client.make_bucket(bucket)
            print(f"Le bucket '{bucket}' a été créé avec succès.")
    except S3Error as exc:
        print(f"Une erreur s'est produite lors de la création du bucket : {exc}")

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
