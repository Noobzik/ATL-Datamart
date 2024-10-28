from minio import Minio
import urllib.request
import pandas as pd
import sys

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # Base URL for NYC Yellow Taxi data files
    base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{month:02d}.csv"
    down_folder = "../../data/raw"
    os.makedirs(down_folder, exist_ok=True)  # Ensure download folder exists

    # Start with recent data and move backward
    present_date = datetime.now()
    year, month = present_date.year, present_date.month - 1  # Start with the previous month
    files_to_download = 10
    downloaded_files = 0

    while downloaded_files < files_to_download:
        # Format the URL and file name
        file_url = base_url.format(year=year, month=month)
        file_name = f"yellow_taxi_{year}-{month:02d}.csv"
        file_path = os.path.join(down_folder, file_name)

        # Attempt to download the file
        try:
            response = requests.get(file_url)
            response.raise_for_status()  # Check if the request was successful

            # Save the file
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_name}")

            downloaded_files += 1
        except requests.HTTPError as e:
            print(f"Failed to download: {file_name} (URL may not exist): {e}")

        # Adjust to the previous month
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    print(f"{downloaded_files} files downloaded to '{down_folder}'.")

if __name__ == '__main__':
    sys.exit(main())


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
    sys.exit(main())
