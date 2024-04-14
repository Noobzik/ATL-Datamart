import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
from io import BytesIO

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a DataFrame to the DBMS engine.

    Parameters:
        - dataframe (pd.DataFrame): The DataFrame to dump into the DBMS engine.

    Returns:
        - bool: True if the connection to the DBMS and the dump to the DBMS is successful, False if either execution fails.
    """
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
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success = True
            print("Connection successful! Processing Parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success = False
        print(f"Error connecting to the database: {e}")
        return success

    return success

def get_parquet_files_from_minio(bucket_name: str) -> list:
    """
    Retrieves Parquet files from Minio.

    Parameters:
        - bucket_name (str): The name of the Minio bucket.

    Returns:
        - list: A list of Parquet file objects retrieved from Minio.
    """
    minio_client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    parquet_files = []
    objects = minio_client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            parquet_files.append(obj)

    return parquet_files

def main() -> None:
    bucket_name = "nycwarehouse"  # Remplacez "votre-bucket-minio" par le nom de votre bucket Minio


    # Récupérer les fichiers Parquet depuis Minio
    parquet_objects = get_parquet_files_from_minio(bucket_name)

    for parquet_obj in parquet_objects:
        try:
            # Télécharger le fichier Parquet depuis Minio
            parquet_data = BytesIO()
            minio_client = Minio(
                "localhost:9000",
                access_key="minio",
                secret_key="minio123",
                secure=False
            )
            minio_client.get_object(bucket_name, parquet_obj.object_name, parquet_data)
          

    
            # Lire le fichier Parquet en tant que DataFrame
            parquet_df = pd.read_parquet(BytesIO(parquet_data.read()))

            # # Nettoyer les noms de colonnes et écrire les données dans PostgreSQL
            # parquet_df.columns = map(str.lower, parquet_df.columns)
            # if not write_data_postgres(parquet_df):
            #     return

            # print(f"Data from {parquet_obj.object_name} dumped to PostgreSQL.")

        except Exception as e:
            print(f"Error processing {parquet_obj.object_name}: {e}")

if __name__ == '__main__':
    sys.exit(main())

