from minio import Minio
import urllib.request
import pandas as pd
import sys
import os

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method downloads specific files of the New York Yellow Taxi. 
    
    Files are saved into the "data/raw" folder.
    """
    download_parquet_files()

def download_parquet_files():
    """Télécharge les fichiers yellow_tripdata_2024-10, -11, -12 et les stocke dans data/raw, uniquement s'ils n'existent pas déjà."""
    urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet"
    ]
    
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    for url in urls:
        filename = url.split("/")[-1]
        filepath = os.path.join(output_dir, filename)
        
        if os.path.exists(filepath):
            print(f"⏭️  {filename} déjà présent, téléchargement ignoré.")
            continue

        print(f"Téléchargement de {filename}...")
        urllib.request.urlretrieve(url, filepath)
        print(f"✅ Fichier téléchargé et sauvegardé dans {filepath}")

def write_data_minio():
    """
    Cette méthode uploade tous les fichiers Parquet de data/raw vers le bucket Minio
    """
    # Configuration Minio
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "nyc-taxi"
    
    # Vérifie si le bucket existe
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✅ Bucket '{bucket}' créé.")
    else:
        print(f"ℹ️ Bucket '{bucket}' existe déjà.")

    # Dossier contenant les fichiers .parquet
    raw_dir = os.path.join("data", "raw")
    files = [f for f in os.listdir(raw_dir) if f.endswith(".parquet")]

    # Upload chaque fichier
    for file in files:
        file_path = os.path.join(raw_dir, file)
        print(f"📤 Upload de {file} vers le bucket {bucket}...")
        client.fput_object(
            bucket_name=bucket,
            object_name=file,
            file_path=file_path,
            content_type="application/octet-stream"
        )
        print(f"✅ {file} uploadé avec succès.")

if __name__ == '__main__':
    sys.exit(main())
