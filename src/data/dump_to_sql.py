import gc
import os
import sys
import logging
import math
import pandas as pd
from minio_operations import connect_to_minio, get_parquet_files_from_minio
from database_operations import connect_to_database, disconnect_from_database, drop_table, initialize_datamart
from grab_parquet import grab_data

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def write_data_postgres(engine, dataframe: pd.DataFrame, chunk_size: int = 10000) -> bool:
    """
    Dumps a Dataframe to the DBMS engine in chunks

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine
        - chunk_size (int): The size of each chunk for batch processing

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    try:
        with engine.connect():
            logging.info("Connection successful! Processing parquet file")

            total_chunks = math.ceil(len(dataframe) / chunk_size)  # Calculate total chunks

            # Split dataframe into chunks and write to database
            for i in range(0, len(dataframe), chunk_size):
                chunk = dataframe[i:i+chunk_size]
                chunk.to_sql('nyc_raw', engine, index=False, if_exists='append')
                logging.info(f"Chunk {i//chunk_size + 1}/{total_chunks} of size {len(chunk)} written to database")
            return True

    except Exception as e:
        logging.error(f"Error connection to the database: {e}")
        return False


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


def process_parquet_files(engine, folder_path: str, files_from_minio: bool) -> None:
    """
    Process Parquet files located in the given folder.

    Parameters:
        - folder_path (str): The path to the folder containing Parquet files.
        - files_from_minio (bool): Indicates whether the files are from Minio or not.
    """
    parquet_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.parquet')]

    for file_number, parquet_file in enumerate(parquet_files, start=1):
        logging.info(f"File {file_number}/{len(parquet_files)} Processing {parquet_file}")
        parquet_df = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')

        if parquet_df.empty:
            logging.warning(f"The DataFrame from {parquet_file} is empty.")
            continue

        parquet_df.columns = map(str.lower, parquet_df.columns)  # Clean column names

        if not write_data_postgres(engine, parquet_df):
            logging.error(f"Error processing {parquet_file}")
            continue

        logging.info(f"{parquet_file} processed successfully.")

        # If files were downloaded from Minio, delete them after processing
        if files_from_minio:
            try:
                os.remove(os.path.join(folder_path, parquet_file))
                logging.info(f"{parquet_file} deleted successfully.")
            except Exception as e:
                logging.error(f"Error deleting {parquet_file}: {e}")


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Get data from Minio
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'tmp')
    files_from_minio = True
    has_parquet_files = get_parquet_files_from_minio(folder_path)

    if not has_parquet_files: # If no parquet files are found in Minio, get them locally
        files_from_minio = False
        folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')
        grab_data()

    # Connect to the warehouse database
    warehouse_engine = connect_to_database(os.getenv('DBMS_WAREHOUSE_DATABASE'))
    if warehouse_engine is None:
        sys.exit(1)

    # Drop the table if it exists
    drop_table(warehouse_engine, 'nyc_raw')

    # Process the parquet files
    process_parquet_files(warehouse_engine, folder_path, files_from_minio)

    # Initialize the datamart database
    if initialize_datamart(folder_path):
        logging.info("Datamart initialized successfully.")
    else:
        logging.error("Error initializing the datamart.")
        sys.exit(1)

    
    # Disconnect from the databases
    disconnect_from_database(warehouse_engine)




if __name__ == '__main__':
    sys.exit(main())
