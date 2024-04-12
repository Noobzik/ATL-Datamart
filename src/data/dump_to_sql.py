import argparse
import gc
import os
import subprocess
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
import psycopg2

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
            print("Data dumped successfully!")

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


def main(start_date) -> None:
    # folder_path: str = r'..\..\data\raw'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]
    # recupere les fichiers parquet ou la date est superieur a la date de debut de traitement
    filtered_files = []
    for parquet_file in parquet_files:
        file_date_str = parquet_file.split('_')[-1].split('.')[0]  # Extraire la date du nom du fichier
        if file_date_str >= start_date:
            filtered_files.append(parquet_file)

    for parquet_file in filtered_files:
        parquet_df: pd.DataFrame = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')

        clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return
        del parquet_df
        gc.collect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Description de votre script main2')
    parser.add_argument('start_date', type=str, help='Description du premier paramètre')
    args = parser.parse_args()
    main(args.start_date)
