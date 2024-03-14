from minio import Minio
import urllib.request
import pandas as pd
import sys
from datetime import datetime, timedelta

def main():
    grab_data()


def grab_data() -> None:
    start_date = datetime(2018, 1, 1)
    end_date = datetime.now()
    current_date = start_date
    while current_date < end_date:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_date.strftime('%Y-%m')}.parquet"
        filename = f"../../data/raw/yellow_tripdata_{current_date.strftime('%Y-%m')}.parquet"
        print(f"Downloading data for {current_date.strftime('%B %Y')}...")
        urllib.request.urlretrieve(url, filename)
        current_date = current_date.replace(month=current_date.month + 1, day=1)


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
