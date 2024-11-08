import requests
from bs4 import BeautifulSoup
from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
import urllib.request
import ssl
import os
import gc

# Configuration pour Minio et PostgreSQL
minio_client = Minio(
    "127.0.0.1:9000",
    secure=False,
    access_key="minio",
    secret_key="minio123"
)
bucket_name = "yellow-taxi-data"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

db_config = {
    "dbms_engine": "postgresql",
    "dbms_username": "postgres",
    "dbms_password": "admin",
    "dbms_ip": "localhost",
    "dbms_port": "15432",
    "dbms_database": "nyc_warehouse",
    "dbms_table": "nyc_raw"
}
db_config["database_url"] = (
    f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
    f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
)

# URL de la page contenant les fichiers Parquet
page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
local_temp_dir = "C:\\temp"
os.makedirs(local_temp_dir, exist_ok=True)


def get_yellow_taxi_links(url):
    """ Récupère tous les liens Parquet pour 2024 depuis la page spécifiée. """
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    links = []
    for link in soup.find_all("a", href=True):
        href = link['href'].strip()
        if "yellow_tripdata_2024" in href and href.endswith(".parquet"):
            if href.startswith("http"):
                links.append(href)
            else:
                links.append(f"https://www.nyc.gov{href}")

    return links


def download_and_upload_to_minio(parquet_url):
    """ Télécharge un fichier Parquet et l'upload dans Minio. """
    file_name = parquet_url.split("/")[-1]
    local_file_path = os.path.join(local_temp_dir, file_name)

    context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(parquet_url, context=context) as response, open(local_file_path, 'wb') as out_file:
            out_file.write(response.read())
        minio_client.fput_object(bucket_name, file_name, local_file_path)
    except Exception as e:
        print(f"Erreur lors du téléchargement ou de l'upload de {file_name} : {e}")
    finally:
        os.remove(local_file_path)  # Supprime le fichier local après l'upload


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """ Charge un DataFrame dans PostgreSQL. """
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
            print("Données insérées avec succès dans PostgreSQL.")
            return True
    except Exception as e:
        print(f"Erreur lors de l'insertion dans PostgreSQL : {e}")
        return False


def transfer_from_minio_to_postgres():
    """ Télécharge chaque fichier depuis Minio et l'insère dans PostgreSQL. """
    objects = minio_client.list_objects(bucket_name)

    for obj in objects:
        file_name = obj.object_name
        local_file_path = os.path.join(local_temp_dir, file_name)
        
        # Télécharge le fichier depuis Minio vers un répertoire temporaire
        minio_client.fget_object(bucket_name, file_name, local_file_path)
        
        # Charge le fichier dans un DataFrame
        parquet_df = pd.read_parquet(local_file_path)
        clean_column_name(parquet_df)
        
        # Insère les données dans PostgreSQL
        if not write_data_postgres(parquet_df):
            print(f"Erreur lors de l'insertion des données de {file_name} dans PostgreSQL.")
        
        # Nettoyage de la mémoire et suppression du fichier local
        del parquet_df
        os.remove(local_file_path)
        gc.collect()


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """ Met les noms des colonnes d'un DataFrame en minuscule. """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


if __name__ == "__main__":
    # Étape 1 : Récupérer tous les liens Parquet pour 2024
    yellow_taxi_links = get_yellow_taxi_links(page_url)

    # Étape 2 : Télécharger chaque fichier et l'uploader dans Minio
    for parquet_link in yellow_taxi_links:
        download_and_upload_to_minio(parquet_link)

    # Étape 3 : Transférer chaque fichier depuis Minio vers PostgreSQL
    transfer_from_minio_to_postgres()
