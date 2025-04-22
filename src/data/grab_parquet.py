from minio import Minio
import urllib.request
import pandas as pd
import sys

def main():
    # recuperer les fichiers parquet de New York Yellow Taxi
    grab_data()
    # uploader les fichiers parquet dans Minio
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # Base URL pour les fichiers parquet
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    
    # Créer le dossier data/raw s'il n'existe pas
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_path = os.path.join(script_dir, '..', '..', 'data', 'raw')
    os.makedirs(raw_data_path, exist_ok=True)
    # os.makedirs("../../data/raw", exist_ok=True)
    
    # Télécharger les fichiers de octobre à decombre 2024

    for month in range(10, 13):  
        # Construire l'URL
        file_name = f"yellow_tripdata_2024-{month:02d}.parquet"
        url = base_url + f"2024-{month:02d}.parquet"
        
        # Chemin local où sauvegarder le fichier
        # local_path = f"../../data/raw/{file_name}"
        local_path = os.path.join(raw_data_path, file_name)
        
        try:
            print(f"Téléchargement de {url}...")
            # Télécharger le fichier
            urllib.request.urlretrieve(url, local_path)
            print(f"Fichier téléchargé avec succès: {local_path}")
        except Exception as e:
            print(f"Erreur lors du téléchargement de {url}: {e}")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    """
    import os
    
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "nyc-taxi-data"  # Choisissez un nom de bucket
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} créé avec succès")
    else:
        print(f"Bucket {bucket} existe déjà")
    
    # Chemin vers le dossier contenant les fichiers téléchargés
    # raw_data_path = "../../data/raw"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_path = os.path.join(script_dir, '..', '..', 'data', 'raw')
    
    # Uploader chaque fichier parquet
    for file_name in os.listdir(raw_data_path):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(raw_data_path, file_name)
            
            try:
                print(f"Upload de {file_path} vers Minio...")
                # Uploader le fichier vers Minio
                client.fput_object(bucket, file_name, file_path)
                print(f"Fichier uploadé avec succès: {file_name}")
            except Exception as e:
                print(f"Erreur lors de l'upload de {file_name}: {e}")

if __name__ == '__main__':
    sys.exit(main())
