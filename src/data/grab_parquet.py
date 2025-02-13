from minio import Minio
import urllib.request
from datetime import date
import pandas as pd
import os
import sys

def main():
    grab_data()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method downloads a Parquet file of the New York Yellow Taxi data for the latest available month.
    The file is saved into the "../../data/raw" folder.
    This method takes no arguments and returns nothing.
    """
    # souce fichier novembre 2024
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Récupérer le mois et l'année actuelle
    current_date = date.today()
    year = current_date.year
    month = current_date.month - 4  # Prendre le mois d'il y a 4 mois
    
    # Ajuster l'année si nécessaire
    if month <= 0:
        month += 12
        year -= 1
    
    month = f"{month:02d}"  # Formater le mois sur deux chiffres
    
    # Construire le nom du fichier Parquet
    file_name = "yellow_taxi_latest.parquet"  # Nom défini
    file_url = base_url + f"yellow_tripdata_{year}-{month}.parquet"
    
    # Définir le dossier de destination
    save_path = os.path.join("..", "..", "data", "raw", file_name)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    try:
        print(f"Téléchargement de {file_url}...")
        urllib.request.urlretrieve(file_url, save_path)
        print(f"Fichier téléchargé et sauvegardé sous {save_path}, remplacement effectué si nécessaire.")
    except Exception as e:
        print(f"Erreur lors du téléchargement du fichier: {e}")


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
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())

