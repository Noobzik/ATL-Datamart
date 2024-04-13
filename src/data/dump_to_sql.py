import gc
import os
import sys
import subprocess
from io import BytesIO

import pandas as pd
from minio import Minio
from sqlalchemy import create_engine


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "admin",
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
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def main() -> None:
    minio_config = {
        "endpoint": "localhost:9000",
        "access_key": "minio",
        "secret_key": "minio123",
        "bucket_name": "nyc-taxi-data"
    }

    try:
        minio_client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=False
        )
        # Récupère la liste des objets stockés dans le bucket Minio spécifié.
        parquet_files = minio_client.list_objects(minio_config['bucket_name'], prefix='', recursive=True)
        # Parcourt tous les objets Parquet trouvés dans le bucket Minio.
        for parquet_file in parquet_files:
            # Récupère le contenu de l'objet Parquet.
            object_data = minio_client.get_object(minio_config['bucket_name'], parquet_file.object_name)
            # Lit le contenu de l'objet Parquet.
            parquet_df: pd.DataFrame = pd.read_parquet(BytesIO(object_data.read()), engine='pyarrow')
            # Nettoie les noms de colonnes du DataFrame.
            clean_column_name(parquet_df)
            # Écrit les données du DataFrame dans la base de données.
            if not write_data_postgres(parquet_df):
                del parquet_df
                gc.collect()
                return
            # Libère la mémoire en supprimant le DataFrame.
            del parquet_df
            gc.collect()

        print("All Parquet files processed successfully")  # Ajout du message de confirmation

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
