import gc
import os
import sys
import io
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.minio_utils import get_minio_client

def list_parquet_files_from_minio(bucket_name):
    """
    Liste tous les fichiers parquet dans le bucket spécifié
    """
    client = get_minio_client()
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        return [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
    except Exception as e:
        print(f"Erreur lors de la liste des objets dans le bucket {bucket_name}: {e}")
        return []



def download_parquet_from_minio(bucket_name, object_name):
    """
    Télécharge un fichier parquet depuis Minio et le retourne comme un DataFrame
    """
    client = get_minio_client()
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        df = pd.read_parquet(io.BytesIO(data), engine='pyarrow')
        print(f"Fichier {object_name} téléchargé avec succès, {len(df)} lignes")
        return df
    except Exception as e:
        print(f"Erreur lors du téléchargement de {object_name}: {e}")
        return None
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

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
        with engine.connect() as conn:
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
            return success

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
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
    bucket_name = "nyc-taxi-data"  # Remplacez par le nom de votre bucket
    
    # Lister tous les fichiers parquet dans le bucket
    parquet_files = list_parquet_files_from_minio(bucket_name)
    
    if not parquet_files:
        print(f"Aucun fichier parquet trouvé dans le bucket {bucket_name}")
        return
    
    for parquet_file in parquet_files:
        print(f"Traitement du fichier: {parquet_file}")
        
        # Télécharger le fichier depuis Minio et le charger en DataFrame
        parquet_df = download_parquet_from_minio(bucket_name, parquet_file)
        
        if parquet_df is None:
            print(f"Impossible de traiter {parquet_file}, passage au fichier suivant")
            continue
        
        # Nettoyer les noms de colonnes
        clean_column_name(parquet_df)
        
        # Écrire les données dans PostgreSQL
        if not write_data_postgres(parquet_df):
            print(f"Échec du traitement pour {parquet_file}")
            del parquet_df
            gc.collect()
            continue
        
        print(f"Fichier {parquet_file} traité avec succès")
        del parquet_df
        gc.collect()

if __name__ == '__main__':
    sys.exit(main())
