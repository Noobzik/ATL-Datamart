import gc
import sys
from io import BytesIO

import psycopg2
from psycopg2 import sql

import pandas as pd
from minio import Minio
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def create_database_if_not_exits():
    try:
        # Connexion au serveur PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",
            user="admin",
            password="admin",
            host="localhost",
            port="15432",
        )
        conn.autocommit = True
        print("Connexion au serveur PostgreSQL réussie.")

        # Vérification de l'existence de la base de données
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'nyc_warehouse';")
        exists = cur.fetchone()
        cur.close()

        if not exists:
            # Création de la base de données si elle n'existe pas
            cur = conn.cursor()
            cur.execute(sql.SQL('CREATE DATABASE nyc_warehouse;'))
            cur.close()
            print("Base de données 'nyc_warehouse' créée avec succès.")
        else:
            print("La base de données 'nyc_warehouse' existe déjà.")

    except OperationalError as e:
        print(f"Error: {e}")

    finally:
        if conn is not None:
            conn.close()



def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """

    # Configuration de la base de données PostgreSQL
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

    create_database_if_not_exits()

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
        "bucket_name": "nyc-taxi-trip"
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
