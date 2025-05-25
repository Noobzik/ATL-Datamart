import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine


MINIO_ENDPOINT = "http://127.0.0.1:9000/"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET_NAME = "taxi"
OBJECT_NAME = "yellow_taxi_latest.parquet"  # Nom exact du fichier
S3_URL = f"s3://{BUCKET_NAME}/{OBJECT_NAME}"

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


def exportToSql() -> None:
    # folder_path: str = r'..\..\data\raw'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    """
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]
    """
    df = pd.read_parquet(
        S3_URL,
        storage_options={
            "key": ACCESS_KEY,
            "secret": SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        }
    )
    parquet_df: pd.DataFrame = df

    clean_column_name(parquet_df)
    if not write_data_postgres(parquet_df):
        del parquet_df
        gc.collect()
        return

    del parquet_df
    gc.collect()


if __name__ == '__main__':
    sys.exit(exportToSql())
