import gc
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from minio import Minio



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

def get_parquet_files_from_minio(bucket_name: str) -> list:
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
    bucket_name = "nycwarehouse"  

    parquet_objects = get_parquet_files_from_minio(bucket_name)

    for parquet_obj in parquet_objects:
            parquet_data = []
            minio_client = Minio(
                "localhost:9000",
                access_key="minio",
                secret_key="minio123",
                secure=False
            )
            object_data = minio_client.get_object(bucket_name, parquet_obj.object_name, parquet_data)
            parquet_df: pd.DataFrame = pd.read_parquet(BytesIO(object_data.read()), engine='pyarrow')
            clean_column_name(parquet_df)
            if not write_data_postgres(parquet_df):
                del parquet_df
                gc.collect()
                return

            del parquet_df
            gc.collect()

    print("All Parquet files processed successfully")  # Ajout du message de confirmation

if __name__ == '__main__':
    sys.exit(main())