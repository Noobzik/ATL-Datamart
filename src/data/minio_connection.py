import os
from minio import Minio
import logging
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