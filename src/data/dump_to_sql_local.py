import gc
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio

# Configuration Minio
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET_NAME = "yellow-taxi-data"

# Configuration PostgreSQL
DB_HOST = "localhost"
DB_PORT = "15432"
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_NAME = "nyc_warehouse"
DB_TABLE = "nyc_raw"

def download_parquet_from_minio(parquet_file: str, download_path: str) -> None:
    """Télécharge un fichier Parquet depuis Minio."""
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    try:
        print(f"Téléchargement de {parquet_file} depuis Minio...")
        client.fget_object(MINIO_BUCKET_NAME, parquet_file, download_path)
        print(f"Téléchargement terminé : {parquet_file}")
    except Exception as e:
        print(f"Erreur lors du téléchargement de {parquet_file} : {e}")

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """Charge le DataFrame dans PostgreSQL."""
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(db_url)
        with engine.connect():
            print("Connexion à PostgreSQL réussie.")
            dataframe.to_sql(DB_TABLE, engine, index=False, if_exists='append')
            print("Données chargées dans PostgreSQL.")
        return True
    except Exception as e:
        print(f"Erreur lors de l'insertion dans PostgreSQL : {e}")
        return False

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les noms de colonnes en les mettant en minuscules."""
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def main() -> None:
    # Dossier local où les fichiers seront téléchargés
    local_dir = os.path.join(os.path.expanduser('~'), 'Desktop', 'tp new york')

    # Liste des fichiers Parquet à traiter
    parquet_files = [f"yellow_tripdata_2024-{month:02d}.parquet" for month in range(1, 9)]

    for parquet_file in parquet_files:
        local_path = os.path.join(local_dir, parquet_file)
        download_parquet_from_minio(parquet_file, local_path)

        if os.path.isfile(local_path):
            print(f"Lecture du fichier Parquet : {local_path}")
            parquet_df = pd.read_parquet(local_path, engine='pyarrow')
            parquet_df = clean_column_name(parquet_df)

            if not write_data_postgres(parquet_df):
                del parquet_df
                gc.collect()
                return

            del parquet_df
            gc.collect()

if __name__ == '__main__':
    sys.exit(main())
