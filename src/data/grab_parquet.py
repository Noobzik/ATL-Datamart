import os
import urllib.request

def execute_data_ingestion():
    """
    Fonction principale qui orchestre la récupération des données NYC Taxi 
    et leur envoi vers le stockage MinIO.
    """
    download_nyc_taxi_files()
    upload_files_to_minio()


def download_nyc_taxi_files():
    """
    Récupère les fichiers de données NYC Taxi et les place dans un dossier local.
    Le dossier de destination est configuré ici comme un sous-dossier du bureau de l'utilisateur.
    """
    from pathlib import Path

    # Définition du répertoire local de destination
    desktop_path = Path.home() / "Desktop"
    destination_dir = desktop_path / "nyc_taxi_dataset"
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Base URL pour le téléchargement des fichiers
    Baseurls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet"
    ]

    for url in urls:
        file_name = url.split("/")[-1]
        local_file_path = destination_dir / file_name

        if local_file_path.exists():
            print(f"ℹ {file_name} déjà présent. Aucun téléchargement nécessaire.")
            continue

        try:
            print(f"⬇ Téléchargement de {file_name}...")
            urllib.request.urlretrieve(url, local_file_path)
            print(f" Enregistré dans : {local_file_path}")
        except Exception as e:
            print(f" Erreur lors du téléchargement de {file_name} : {e}")


        try:
            print(f"⬇ Téléchargement en cours : {filename}")
            urllib.request.urlretrieve(url, target_path)
            print(f" Fichier sauvegardé : {target_path}")
        except Exception as error:
            print(f" Échec lors du téléchargement de {filename} : {error}")
