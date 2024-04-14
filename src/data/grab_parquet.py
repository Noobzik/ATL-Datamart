import os
import sys
import pandas as pd
from minio import Minio
import urllib.request
import pendulum
import logging
from urllib.error import URLError, HTTPError
from minio_operations import connect_to_minio
from dotenv import load_dotenv

load_dotenv()

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = f"{os.getenv('DATA_SOURCE_BASE_URL')}/trip-data"
DATA_DIR = "../../data/raw"

def main():
    """Main function to grab the data from New York Yellow Taxi
    This function is the entry point for the grab_parquet.py script.
    """
    # Grab data
    logging.info("Starting data retrieval process...")
    grab_data()
    logging.info("Data retrieval process completed.")

    # Grab last data
    logging.info("Starting last data retrieval process...")
    grab_last_data()
    logging.info("Last data retrieval process completed.")

    # Write data to Minio
    logging.info("Starting data upload to Minio...")
    write_data_minio()
    logging.info("Data upload to Minio completed.")
    

def file_exists(file_path: str) -> bool:
    """Check if a file exists.

    Parameters:
        - file_path (str): The path to the file to check.

    Returns:
        - bool: True if the file exists, False otherwise.
    """
    return os.path.exists(file_path)


def grab_last_data(max_attempts = 12) -> None:
    """Grab the last data from New York Yellow Taxi
    This function attempts to download the most recent data available from the New York Yellow Taxi dataset.

    Parameters:
        - max_attempts (int): The maximum number of attempts to download the last data.
    
    Returns:
        - None
    """
    # Get the current date
    date = pendulum.now()

    # Loop to search for the last available file
    attempts = 0
    while attempts < max_attempts:
        year = date.year
        month = date.month

        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_path = os.path.join(DATA_DIR, file_name)

        if not file_exists(file_path): # If the file does not exist, download it
            url = f"{BASE_URL}/{file_name}" # Construct the source URL
            try:
                urllib.request.urlretrieve(url, file_path) # Download the file
                logging.info(f"{month:02d} {year} data downloaded successfully.")
                break
            except (URLError, HTTPError) as e: # Handle errors
                logging.error(f"Error downloading {month:02d} {year} data: {str(e)}")
        else: # If the file exists, skip the download
            logging.info(f"{file_name} already exists. Skipping download.")
            break

        date = date.subtract(months=1) # Move to the previous month
        attempts += 1 # Increment the attempts counter
        logging.info(f"Attempt {attempts}/{max_attempts}")



def grab_data() -> None:
    """Grab the data from New York Yellow Taxi
    This function downloads datasets from November 2023 to December 2023 of the New York Yellow Taxi.

    Returns:
        - None
    """
    # Months and years to retrieve
    months = range(11, 13) # November to December
    years = [2023]

    # Download the data
    for year in years: # This loop is not necessary for the current implementation but can be useful for future updates (e.g., multiple years)
        for month in months:
            file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
            file_path = os.path.join(DATA_DIR, file_name)

            if not file_exists(file_path): # If the file does not exist, download it
                url = f"{BASE_URL}/{file_name}" # Construct the source URL
                try:
                    urllib.request.urlretrieve(url, file_path) # Download the file
                    logging.info(f"{month:02d} {year} data downloaded successfully.")
                except (URLError, HTTPError) as e: # Handle errors
                    logging.error(f"Error downloading {month:02d} {year} data: {str(e)}")
            else: # If the file exists, skip the download
                logging.info(f"{file_name} already exists. Skipping download.")


def exists_in_minio(minio_client: Minio, bucket: str, file: str) -> bool:
    """
    This method checks if the data already exists in Minio

    Parameters:
        - minio_client (Minio): The Minio client.
        - bucket (str): The bucket to check.
        - file (str): The file to check.

    Returns:
        - bool: True if the data already exists in Minio, False otherwise.
    """
    try:
        minio_client.stat_object(bucket, file, None)
        return True
    except Exception as e:
        return False


def write_data_minio():
    """
    This method put all Parquet files into Minio

    Returns:
        - None
    """
    # Connect to Minio
    minio_client = connect_to_minio()
    if minio_client is None: # If the connection fails, exit the script
        sys.exit(1)
    
    # Create the bucket if it does not exist
    bucket: str = os.getenv("MINIO_BUCKET")
    found = minio_client.bucket_exists(bucket)
    if not found:
        minio_client.make_bucket(bucket)
        logging.info("Bucket " + bucket + " created")
    else:
        logging.info("Bucket " + bucket + " found")
    
    # Upload all Parquet files to Minio
    for file in os.listdir(DATA_DIR): # Get all files in the data directory to save them in Minio
        if file.endswith(".parquet"):
            file_path = os.path.join(DATA_DIR, file)
            if not exists_in_minio(minio_client, bucket, file): # If the file does not exist in Minio, upload it
                minio_client.fput_object(bucket, file, file_path)
                logging.info(f"{file} uploaded to Minio")
            else: # If the file exists in Minio, skip the upload
                logging.info(f"{file} already exists in Minio. Skipping upload")
            


if __name__ == '__main__':
    logging.info("Starting data retrieval process...")
    sys.exit(main())