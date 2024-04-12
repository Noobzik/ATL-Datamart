import os
import sys
from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
import gc
from urllib.parse import quote_plus

def download_files_from_minio(bucket_name, local_directory):
    minio_client = Minio("localhost:9000",
                         access_key="minio",
                         secret_key="minio123",
                         secure=False)

    objects = minio_client.list_objects(bucket_name)
    for obj in objects:
        local_path = os.path.join(local_directory, obj.object_name)
        minio_client.fget_object(bucket_name, obj.object_name, local_path)
        print(f"Downloaded: {obj.object_name} to {local_path}")

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "postgres",
        "dbms_ip": "localhost",
        "dbms_port": "5432",
        "dbms_database": "datalake",
        "dbms_table": "nyc"
    }
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
    except Exception as e:
        success = False
        print(f"Error connection to the database: {e}")
        return success

    return success

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')
    os.makedirs(folder_path, exist_ok=True)

    download_files_from_minio("datalake", folder_path)

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        parquet_df = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')
        parquet_df = clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()

if __name__ == '__main__':
    sys.exit(main())
