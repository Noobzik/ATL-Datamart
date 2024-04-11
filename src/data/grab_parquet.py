import os
import sys
import pandas as pd
from minio import Minio
import urllib.request
import pendulum
import logging
from urllib.error import URLError, HTTPError

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = "../../data/raw"

def main():
    """Main function to grab the data from New York Yellow Taxi
    This function is the entry point for the grab_parquet.py script.
    """
    # Grab data
    logging.info("Starting 2023 data retrieval process...")
    grab_data()
    logging.info("2023 data retrieval process completed.")

    # Grab last data
    logging.info("Starting last data retrieval process...")
    grab_last_data()
    logging.info("Last data retrieval process completed.")
    

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
    This function downloads datasets from January 2023 to December 2023 of the New York Yellow Taxi.

    Returns:
        - None
    """
    # Months and years to retrieve
    months = range(1, 13) # January to December
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




def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    logging.info("Starting data retrieval process...")
    sys.exit(main())