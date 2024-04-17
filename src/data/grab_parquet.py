import subprocess
from minio import Minio
import urllib.request
from datetime import datetime, timedelta

def main(start_date, end_date):
    grab_data(start_date, end_date)
    write_data_minio()
    start_date_str = start_date.strftime('%Y-%m-%d')
    subprocess.call(["python", "dump_to_sql.py", start_date_str])
    subprocess.call(["python", "insertion_data.py", start_date_str])


def grab_data(start_date, end_date) -> None:
    current_date = start_date
    while current_date < end_date:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_date.strftime('%Y-%m')}.parquet"
        filename = f"../../data/raw/yellow_tripdata_{current_date.strftime('%Y-%m')}.parquet"
        print(f"Downloading data for {current_date.strftime('%B %Y')}...")
        print(f"URL: {url}", f"Filename: {filename}")
        urllib.request.urlretrieve(url, filename)
        current_date = current_date.replace(month=current_date.month + 1, day=1)
        print("Data downloaded successfully!")

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
    bucket: str = "taxis-parquet"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    # Put all parquet files into Minio
    files = subprocess.getoutput("ls ../../data/raw").split("\n")
    for file in files:
        if file.endswith(".parquet"):
            try:
                # Check if the file exists in the bucket
                client.stat_object(bucket, file)
                print(f"{file} already exists in Minio")
            except Exception as e:
                print(f"Uploading {file} to Minio...")
                # Upload the file to Minio
                client.fput_object(bucket, file, f"../../data/raw/{file}")
                print(f"{file} uploaded successfully!")
    print("All files uploaded successfully!")


if __name__ == '__main__':
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2020, 2, 1)
    main(start_date=start_date, end_date=end_date)

