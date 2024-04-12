import os
import logging
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

def connect_to_minio():
    """
    Connect to Minio

    Returns:
        - Minio: The Minio client if the connection is successful, None otherwise.
    """
    try:
        minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            secure=False,
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY")
        )
        logging.info("Connected to Minio successfully.")
        return minio_client

    except Exception as e:
        logging.error(f"Error connecting to Minio: {str(e)}")
        return None


def retrieve_files():
    """
    Retrieve files from Minio

    Returns:
        - None
    """
    # Connect to Minio
    minio_client = connect_to_minio()
    if minio_client is None:
        return False
    
    bucket: str = os.getenv("MINIO_BUCKET")
    objects = minio_client.list_objects(bucket)
    for obj in objects:
        destination_path = os.path.join(os.getenv("DATA_DIR"), obj.object_name)
        try:
            minio_client.fget_object(bucket, obj.object_name, destination_path)
            logging.info(f"{obj.object_name} downloaded from Minio successfully.")

        except Exception as e:
            logging.error(f"Error downloading {obj.object_name} from Minio: {e}")



def get_parquet_files_from_minio(folder_path: str):
    """
    Get Parquet files from Minio

    Parameters:
        - folder_path (str): The folder path to save the Parquet files.

    Returns:
        - success (bool): True if the operation is successful, False otherwise.
    """
    success: bool = True
    # Connect to Minio
    minio_client = connect_to_minio()
    if minio_client is None:
        success = False
    
    bucket: str = os.getenv("MINIO_BUCKET")
    objects = minio_client.list_objects(bucket)
    parquet_files = [obj.object_name for obj in objects if obj.object_name.lower().endswith('.parquet')]

    if not parquet_files:
        logging.error("No parquet files found in Minio.")
        return False

    for parquet_file in parquet_files:
        destination_path = os.path.join(folder_path, parquet_file)
        try:
            minio_client.fget_object(bucket, parquet_file, destination_path)
            logging.info(f"{parquet_file} downloaded from Minio successfully.")

        except Exception as e:
            logging.error(f"Error downloading {parquet_file} from Minio: {e}")
            success = False

    return success