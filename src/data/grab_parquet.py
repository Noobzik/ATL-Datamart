

import requests
from bs4 import BeautifulSoup
from minio import Minio
import urllib.request
import ssl
import os

# URL de la page à scraper
page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Configuration MinIO
minio_client = Minio(
    "127.0.0.1:9000",
    secure=False,
    access_key="minio",
    secret_key="minio123"
)
bucket_name = "yellow-taxi-data"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' créé.")
else:
    print(f"Bucket '{bucket_name}' déjà existant.")


# Fonction pour scraper tous les liens Parquet de Yellow Taxi Trip Records pour 2024
def get_yellow_taxi_links(url):
    response = requests.get(url)
    response.raise_for_status()  # Vérifie que la requête a réussi
    soup = BeautifulSoup(response.text, 'html.parser')

    # Trouver tous les liens pour les fichiers Yellow Taxi de 2024
    links = []
    for link in soup.find_all("a", href=True):
        href = link['href'].strip()
        # Sélectionner uniquement les liens Yellow Taxi de 2024
        if "yellow_tripdata_2024" in href and href.endswith(".parquet"):
            # S'assurer que les liens sont complets
            if href.startswith("http"):
                links.append(href)
            else:
                # Ajoute le domaine si le lien est relatif
                links.append(f"https://www.nyc.gov{href}")

    # Vérifier si tous les mois de janvier à décembre sont présents
    expected_months = {f"yellow_tripdata_2024-{str(month).zfill(2)}" for month in range(1, 13)}
    available_months = {link.split('/')[-1].split('.')[0] for link in links}
    missing_months = expected_months - available_months

    if missing_months:
        print(f"Attention : les mois suivants sont manquants dans les liens extraits : {missing_months}")
    else:
        print("Tous les mois de 2024 sont présents.")

    return links


# Fonction pour télécharger et uploader dans MinIO
def download_and_upload_to_minio(parquet_url):
    # Nom de fichier pour MinIO
    file_name = parquet_url.split("/")[-1]
    local_temp_dir = "C:\\temp"
    local_file_path = os.path.join(local_temp_dir, file_name)

    os.makedirs(local_temp_dir, exist_ok=True)

    # Désactiver la vérification SSL
    context = ssl._create_unverified_context()

    # Télécharger le fichier
    try:
        print(f"Téléchargement de {file_name} depuis {parquet_url}...")
        with urllib.request.urlopen(parquet_url, context=context) as response, open(local_file_path, 'wb') as out_file:
            out_file.write(response.read())
        print(f"{file_name} téléchargé avec succès.")
    except Exception as e:
        print(f"Erreur lors du téléchargement de {file_name} : {e}")
        return

    # Uploader dans MinIO
    try:
        minio_client.fput_object(bucket_name, file_name, local_file_path)
        print(f"Fichier {file_name} uploadé avec succès dans le bucket '{bucket_name}' sur MinIO.")
    except Exception as e:
        print(f"Erreur lors de l'upload de {file_name} dans MinIO : {e}")


# Exécution
if __name__ == "__main__":
    # Obtenir tous les liens Yellow Taxi Parquet pour 2024
    yellow_taxi_links = get_yellow_taxi_links(page_url)
    print(f"Liens Yellow Taxi récupérés : {yellow_taxi_links}")

    # Télécharger et uploader chaque fichier dans MinIO
    for parquet_link in yellow_taxi_links:
        download_and_upload_to_minio(parquet_link)




"""
from minio import Minio
import urllib.request
import pandas as pd
import sys

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # Base URL for NYC Yellow Taxi data files
    base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{month:02d}.csv"
    down_folder = "..\ArchiData1\ATL-Datamart\data\raw"
    os.makedirs(down_folder, exist_ok=True)  # Ensure download folder exists

    # Start with recent data and move backward
    present_date = datetime.now()
    year, month = present_date.year, present_date.month - 1  # Start with the previous month
    files_to_download = 10
    downloaded_files = 0

    while downloaded_files < files_to_download:
        # Format the URL and file name
        file_url = base_url.format(year=year, month=month)
        file_name = f"yellow_taxi_{year}-{month:02d}.csv"
        file_path = os.path.join(down_folder, file_name)

        # Attempt to download the file
        try:
            response = requests.get(file_url)
            response.raise_for_status()  # Check if the request was successful

            # Save the file
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_name}")

            downloaded_files += 1
        except requests.HTTPError as e:
            print(f"Failed to download: {file_name} (URL may not exist): {e}")

        # Adjust to the previous month
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    print(f"{downloaded_files} files downloaded to '{down_folder}'.")

if __name__ == '__main__':
    sys.exit(main())


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
"""
