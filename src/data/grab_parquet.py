from minio import Minio
import urllib.request
import pandas as pd
import os
import sys
import requests

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi"""
    # Dossier de destination
    output_dir = os.path.join(os.path.expanduser('~'), 'Desktop', 'tp new york')
    os.makedirs(output_dir, exist_ok=True)

    # Base URL pour le téléchargement des fichiers
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"

    # Liste des mois à télécharger (jusqu'à août 2024)
    months = ['01', '02', '03', '04', '05', '06', '07', '08']

    for month in months:
        file_url = f"{base_url}{month}.parquet"
        file_path = os.path.join(output_dir, f"yellow_tripdata_2024-{month}.parquet")
        
        try:
            print(f"Téléchargement de {file_url}...")
            response = requests.get(file_url)
            response.raise_for_status()  # Vérifie si la requête a réussi
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Téléchargement réussi : {file_path}")
        except requests.exceptions.HTTPError as e:
            print(f"Erreur lors du téléchargement de {file_url}: {e}")
        except Exception as e:
            print(f"Erreur inattendue lors du téléchargement de {file_url}: {e}")

def write_data_minio():
    """Put all Parquet files into Minio"""
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    
    bucket: str = "yellow-taxi-data"  # Nom valide pour le bucket
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    
    # Dossier de destination
    output_dir = os.path.join(os.path.expanduser('~'), 'Desktop', 'tp new york')
    
    # Téléchargez tous les fichiers Parquet dans le bucket
    for month in range(1, 9):  # De 1 à 8
        file_name = f"yellow_tripdata_2024-{month:02d}.parquet"
        file_path = os.path.join(output_dir, file_name)
        if os.path.exists(file_path):
            print(f"Téléchargement de {file_name} vers Minio...")
            try:
                client.fput_object(bucket, file_name, file_path)
                print(f"{file_name} téléchargé avec succès dans Minio.")
            except Exception as e:
                print(f"Erreur lors du téléchargement de {file_name} dans Minio: {e}")
        else:
            print(f"Le fichier {file_name} n'existe pas et ne peut pas être téléchargé.")

if __name__ == '__main__':
    sys.exit(main())
