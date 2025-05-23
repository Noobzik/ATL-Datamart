from minio import Minio
import urllib.request
import os
from pathlib import Path
import sys

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet"
    ]

    os.makedirs("data/raw", exist_ok=True)

    for url in urls:
        filename = url.split("/")[-1]
        local_path = f"data/raw/{filename}"
        if not os.path.exists(local_path):
            print(f"Téléchargement de {filename}...")
            urllib.request.urlretrieve(url, local_path)
            print(f"✅ {filename} téléchargé dans {local_path}")
        else:
            print(f"⚠️ {filename} déjà présent, téléchargement ignoré.")

def write_data_minio():
    """
    Cette méthode upload tous les fichiers Parquet dans Minio
    """
    print("🚀 Connexion à Minio...")
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )


    bucket = "nyc-taxi"
    print(f"🔍 Vérification du bucket : {bucket}")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✅ Bucket {bucket} créé.")
    else:
        print("ℹ️ Le bucket existe déjà.")

    print("📂 Scan du dossier data/raw...")
    for file in Path("data/raw").glob("*.parquet"):
        object_name = file.name
        print(f"📤 Upload de {object_name} vers Minio...")
        client.fput_object(bucket, object_name, str(file))
        print(f"✅ {object_name} uploadé avec succès.")


if __name__ == '__main__':
    sys.exit(main())
